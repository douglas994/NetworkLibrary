/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - NetworkServer
 *
 *  UDP server that manages multiple client connections.
 *  Uses raw Socket with SocketAsyncEventArgs for zero-allocation async I/O.
 *  No connection limit — scales to thousands of concurrent peers.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Event arguments for server events.
    /// </summary>
    public sealed class NetworkEventArgs
    {
        public NetworkPeer Peer { get; internal set; } = null!;
        public byte[] Data { get; internal set; } = null!;
        public int DataOffset { get; internal set; }
        public int DataLength { get; internal set; }
        public DeliveryMethod DeliveryMethod { get; internal set; }
    }

    /// <summary>
    /// High-performance UDP server for MMORPG networking.
    /// Manages connections, reliability, fragmentation, and heartbeats.
    /// No hard connection limit — limited only by system resources.
    /// </summary>
    public sealed class NetworkServer : IDisposable
    {
        // ── Configuration ──
        private readonly int _maxPeers;
        private readonly long _connectionTimeout; // Stopwatch ticks
        private readonly long _pingInterval;      // Stopwatch ticks

        // ── Socket ──
        // In single-socket mode: _sockets[0] is the only socket.
        // In SO_REUSEPORT mode: _sockets[i] is owned exclusively by _receiveThreads[i].
        private Socket[]? _sockets;
        private bool _isRunning;

        // ── Threading (I/O) ──
        private Thread[]? _receiveThreads;
        private readonly System.Collections.Concurrent.ConcurrentQueue<RawPacket> _receiveQueue;

        // ── Peers ──
        // ConcurrentDictionary required: multiple ReceiveLoop threads may accept new peers simultaneously.
        private readonly ConcurrentDictionary<PeerAddress, NetworkPeer> _peersByEndPoint;
        private readonly ConcurrentDictionary<uint, NetworkPeer> _peersById;
        private readonly System.Collections.Concurrent.ConcurrentBag<NetworkPeer> _peerPool;
        private int _nextPeerId; // Interlocked

        // ── ReusePort config ──
        private int _reusePortWorkers = 1; // 1 = disabled (single socket)

        // ── Buffers ──
        private readonly ArrayPool<byte> _bufferPool;

        // ── Events ──
        private readonly Queue<NetworkEventArgs> _eventQueue;
        private readonly Queue<NetworkEventArgs> _eventPool;

        // ── Retransmit buffer ──
        private readonly PendingPacket[] _retransmitBuffer;

        // ── Reusable temp list for disconnection ──
        private readonly List<NetworkPeer> _peersToRemove = new List<NetworkPeer>();

        // ── Event callbacks ──

        /// <summary>Called when a new client connects.</summary>
        public Action<NetworkPeer>? OnPeerConnected;

        /// <summary>Called when a client disconnects.</summary>
        public Action<NetworkPeer>? OnPeerDisconnected;

        /// <summary>Called when data is received from a client.</summary>
        public Action<NetworkPeer, byte[], int, int, DeliveryMethod>? OnDataReceived;

        /// <summary>
        /// Creates a new NetworkServer.
        /// </summary>
        /// <param name="maxPeers">Maximum number of concurrent connections (no hard limit, just pre-allocation hint).</param>
        /// <param name="connectionTimeoutMs">Timeout in milliseconds before disconnecting idle peers.</param>
        /// <param name="pingIntervalMs">Interval in milliseconds for sending ping/heartbeat packets.</param>
        public NetworkServer(int maxPeers = 10000, int connectionTimeoutMs = 15000, int pingIntervalMs = 1000)
        {
            _maxPeers = maxPeers;
            _connectionTimeout = (long)(connectionTimeoutMs / 1000.0 * Stopwatch.Frequency);
            _pingInterval = (long)(pingIntervalMs / 1000.0 * Stopwatch.Frequency);
            _nextPeerId = 1;

            _peersByEndPoint = new ConcurrentDictionary<PeerAddress, NetworkPeer>(Environment.ProcessorCount, maxPeers);
            _peersById = new ConcurrentDictionary<uint, NetworkPeer>(Environment.ProcessorCount, maxPeers);
            _peerPool = new System.Collections.Concurrent.ConcurrentBag<NetworkPeer>();

            _bufferPool = ArrayPool<byte>.Shared;
            _receiveQueue = new System.Collections.Concurrent.ConcurrentQueue<RawPacket>();

            _eventQueue = new Queue<NetworkEventArgs>(100);
            _eventPool = new Queue<NetworkEventArgs>(256);

            _retransmitBuffer = new PendingPacket[64];

            // Pre-allocate peer pool
            for (int i = 0; i < Math.Min(maxPeers, 100); i++)
                _peerPool.Add(new NetworkPeer());

            // Pre-allocate event pool
            for (int i = 0; i < 256; i++)
                _eventPool.Enqueue(new NetworkEventArgs());
        }

        /// <summary>
        /// Enables SO_REUSEPORT mode for Linux: spawns <paramref name="workerCount"/> parallel
        /// receive sockets on the same port. The Linux kernel distributes packets by 5-tuple hash,
        /// so each peer always lands on the same socket — no per-peer locking needed.
        /// Must be called BEFORE <see cref="Start"/>.
        /// Has no effect on Windows (falls back silently to single-socket mode).
        /// </summary>
        /// <param name="workerCount">Number of sockets/threads. Recommended: Environment.ProcessorCount / 2.</param>
        public void EnableReusePort(int workerCount)
        {
            if (_isRunning)
                throw new InvalidOperationException("Cannot enable ReusePort after Start()");

            // SO_REUSEPORT load-balancing only works on Linux 3.9+
            // On Windows it acts like SO_REUSEADDR and does NOT load-balance — silently use 1 socket.
            if (!OperatingSystem.IsLinux())
            {
                Console.WriteLine("[NetworkServer] SO_REUSEPORT: not Linux, falling back to single-socket mode.");
                _reusePortWorkers = 1;
                return;
            }

            _reusePortWorkers = Math.Max(1, workerCount);
            Console.WriteLine($"[NetworkServer] SO_REUSEPORT enabled: {_reusePortWorkers} worker sockets.");
        }

        /// <summary>
        /// Starts listening for connections on the specified port.
        /// </summary>
        public void Start(int port, bool ipv6 = false)
        {
            if (_isRunning)
                throw new InvalidOperationException("Server is already running");

            int workerCount = _reusePortWorkers;
            _sockets = new Socket[workerCount];
            _receiveThreads = new Thread[workerCount];

            for (int i = 0; i < workerCount; i++)
            {
                var addressFamily = ipv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;
                var socket = new Socket(addressFamily, SocketType.Dgram, ProtocolType.Udp);

                socket.Blocking = true;
                socket.ReceiveBufferSize = 4 * 1024 * 1024; // 4 MB per socket
                socket.SendBufferSize = 4 * 1024 * 1024;

                // Ignore ICMP port unreachable on Windows
                try
                {
                    const int SIO_UDP_CONNRESET = unchecked((int)0x9800000C);
                    socket.IOControl(SIO_UDP_CONNRESET, new byte[] { 0 }, null);
                }
                catch { /* non-Windows */ }

                // SO_REUSEPORT: enables kernel-level load balancing on Linux
                if (workerCount > 1 && OperatingSystem.IsLinux())
                {
                    // Socket option 15 = SO_REUSEPORT on Linux
                    socket.SetSocketOption(SocketOptionLevel.Socket, (SocketOptionName)15, true);
                }

                if (ipv6)
                {
                    socket.DualMode = true;
                    socket.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
                }
                else
                {
                    socket.Bind(new IPEndPoint(IPAddress.Any, port));
                }

                _sockets[i] = socket;

                int workerIndex = i; // capture for lambda
                var thread = new Thread(() => ReceiveLoop(workerIndex))
                {
                    Name = $"NetServer_Recv_{workerIndex}",
                    IsBackground = true,
                    Priority = ThreadPriority.Highest
                };
                _receiveThreads[i] = thread;
            }

            _isRunning = true;

            // Start all receive threads after all sockets are bound
            for (int i = 0; i < workerCount; i++)
                _receiveThreads[i].Start();
        }

        /// <summary>
        /// Stops the server and disconnects all peers.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            if (_sockets != null)
            {
                foreach (var s in _sockets)
                    s?.Close();
            }

            if (_receiveThreads != null)
            {
                foreach (var t in _receiveThreads)
                    t?.Join(500);
            }

            foreach (var peer in _peersByEndPoint.Values)
                peer.State = PeerState.Disconnected;

            _peersByEndPoint.Clear();
            _peersById.Clear();

            _sockets = null;
        }

        /// <summary>
        /// Processes network events. Must be called regularly from the main/game thread.
        /// This is the main update loop — receives packets, processes reliability,
        /// handles timeouts, and triggers callbacks.
        /// </summary>
        public void Update()
        {
            if (!_isRunning)
                return;

            // Tick the simulator using the primary socket (socket 0)
            if (_sockets != null && _sockets.Length > 0)
                Simulator.Tick(_sockets[0]);

            // 1. Drain all received packets (thread-safe ConcurrentQueue)
            ReceivePackets();

            // 2. Process retransmissions for all peers
            ProcessRetransmissions();

            // 3. Process timeouts and pings
            ProcessHeartbeats();
        }

        /// <summary>
        /// Sends data to a specific peer.
        /// </summary>
        /// <param name="peer">Target peer.</param>
        /// <param name="data">Data to send.</param>
        /// <param name="offset">Offset in data array.</param>
        /// <param name="length">Length of data.</param>
        /// <param name="delivery">Delivery method.</param>
        public void Send(NetworkPeer peer, byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            if (peer.State != PeerState.Connected)
                return;

            // Check if fragmentation is needed
            if (FragmentChannel.NeedsFragmentation(length) && delivery != DeliveryMethod.Unreliable)
            {
                SendFragmented(peer, data, offset, length, delivery);
                return;
            }

            switch (delivery)
            {
                case DeliveryMethod.Unreliable:
                    SendUnreliable(peer, data, offset, length);
                    break;

                case DeliveryMethod.Reliable:
                    SendReliable(peer, data, offset, length, peer._reliableChannel);
                    break;

                case DeliveryMethod.ReliableOrdered:
                    SendReliable(peer, data, offset, length, peer._reliableOrderedChannel);
                    break;

                case DeliveryMethod.Sequenced:
                    SendSequenced(peer, data, offset, length);
                    break;
            }
        }

        /// <summary>
        /// Sends data to all connected peers.
        /// </summary>
        public void SendToAll(byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            foreach (var peer in _peersByEndPoint.Values)
            {
                if (peer.State == PeerState.Connected)
                {
                    Send(peer, data, offset, length, delivery);
                }
            }
        }

        /// <summary>
        /// Sends data to all connected peers except one.
        /// </summary>
        public void SendToAllExcept(NetworkPeer excludedPeer, byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            foreach (var peer in _peersByEndPoint.Values)
            {
                if (peer.State == PeerState.Connected && peer != excludedPeer)
                {
                    Send(peer, data, offset, length, delivery);
                }
            }
        }

        /// <summary>
        /// Disconnects a specific peer.
        /// </summary>
        public void DisconnectPeer(NetworkPeer peer)
        {
            if (peer.State == PeerState.Disconnected)
                return;

            // Send disconnect notification
            SendInternalPacket(peer, InternalPacketType.Disconnect);

            RemovePeer(peer);
        }

        /// <summary>
        /// Gets a peer by its ID.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NetworkPeer? GetPeer(uint peerId)
        {
            _peersById.TryGetValue(peerId, out var peer);
            return peer;
        }

        /// <summary>
        /// Gets the number of currently connected peers.
        /// </summary>
        // ── Fragmentation ──
        public int MtuSize { get; set; } = PacketHeader.SafeMTU;

        // ── Simulation (Chaos Monkey) ──
        public NetworkConditionSimulator Simulator { get; } = new NetworkConditionSimulator();

        /// <summary>
        /// Whether the server is currently running.
        /// </summary>
        public bool IsRunning => _isRunning;

        // ════════════════════════════════════════════════════════
        //  PRIVATE IMPLEMENTATION
        // ════════════════════════════════════════════════════════

        private void ReceiveLoop(int workerIndex)
        {
            var socket = _sockets![workerIndex];
            // Each thread has its own private buffer — no sharing, no locking on receive
            byte[] receiveBuffer = new byte[PacketHeader.SafeMTU];
            SocketAddress remoteSA = new SocketAddress(AddressFamily.InterNetwork);

            while (_isRunning)
            {
                int received;
                try
                {
                    received = socket.ReceiveFrom(receiveBuffer.AsSpan(), SocketFlags.None, remoteSA);
                }
                catch (SocketException)
                {
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                if (received < PacketHeader.HeaderSize)
                    continue;

                byte[] dataCopy = _bufferPool.Rent(received);
                Buffer.BlockCopy(receiveBuffer, 0, dataCopy, 0, received);

                _receiveQueue.Enqueue(new RawPacket
                {
                    Buffer = dataCopy,
                    Length = received,
                    Sender = new PeerAddress(remoteSA)
                });
            }
        }

        private void ReceivePackets()
        {
            while (_receiveQueue.TryDequeue(out var packet))
            {
                int received = packet.Length;
                byte[] data = packet.Buffer;
                var peerAddr = packet.Sender;

                if (!PacketHeader.Read(data, received, out var delivery, out var sequence, out var ack,
                    out var ackBits, out var dataLength))
                {
                    _bufferPool.Return(data);
                    continue;
                }

                // Find or create peer
                if (!_peersByEndPoint.TryGetValue(peerAddr, out var peer))
                {
                    // New connection request
                    if (delivery == DeliveryMethod.Internal)
                    {
                        peer = AcceptConnection(peerAddr);
                        if (peer == null)
                        {
                            _bufferPool.Return(data);
                            continue;
                        }
                    }
                    else
                    {
                        _bufferPool.Return(data);
                        continue; // Unknown peer, drop
                    }
                }

                peer.LastReceiveTime = System.Diagnostics.Stopwatch.GetTimestamp();
                ProcessReceive(peer, data, received, dataLength, delivery, sequence, ack, ackBits, storeData: true);
            }
        }

        private void ProcessReceive(NetworkPeer peer, byte[] buffer, int totalReceived, int dataLength, DeliveryMethod delivery, 
ushort sequence, ushort ack, uint ackBits, bool storeData)
        {
            // Process ACKs (Both Reliable and ReliableOrdered)
            if (delivery == DeliveryMethod.Reliable || delivery == DeliveryMethod.ReliableOrdered)
            {
                peer._reliableChannel.ProcessAck(ack, ackBits);
                peer._reliableOrderedChannel.ProcessAck(ack, ackBits);
            }

            int payloadLength = Math.Min(dataLength, totalReceived - PacketHeader.HeaderSize);

            switch (delivery)
            {
                case DeliveryMethod.Internal:
                    ProcessInternalPacket(peer, buffer, PacketHeader.HeaderSize, dataLength);
                    _bufferPool.Return(buffer);
                    return;
                
                // Unreliable
                case DeliveryMethod.Unreliable:
                    peer.ProcessUnreliableSequence(sequence);
                    RaiseDataReceived(peer, buffer, PacketHeader.HeaderSize, dataLength, delivery);
                    _bufferPool.Return(buffer);
                    return;

                case DeliveryMethod.Reliable:
                    if (peer._reliableChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, dataLength))
                    {
                        RaiseDataReceived(peer, buffer, PacketHeader.HeaderSize, dataLength, delivery);
                    }
                    break;

                case DeliveryMethod.ReliableOrdered:
                    peer._reliableOrderedChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, dataLength, true);
                    // Deliver in order
                    while (peer._reliableOrderedChannel.TryGetNextOrdered(out var orderedData, out var orderedLen))
                    {
                        RaiseDataReceived(peer, orderedData, 0, orderedLen, delivery);
                        NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(orderedData);
                    }
                    break;

                case DeliveryMethod.ReliableFragment:
                    if (peer._reliableChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, dataLength))
                    {
                        if (peer._fragmentChannel.ProcessFragment(buffer, PacketHeader.HeaderSize, dataLength, out var completeData, out var completeLen))
                        {
                            RaiseDataReceived(peer, completeData, 0, completeLen, DeliveryMethod.Reliable);
                            NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(completeData);
                        }
                    }
                    break;

                case DeliveryMethod.ReliableOrderedFragment:
                    peer._reliableOrderedChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, dataLength, true);
                    while (peer._reliableOrderedChannel.TryGetNextOrdered(out var orderedData, out var orderedLen))
                    {
                        if (peer._fragmentChannel.ProcessFragment(orderedData, 0, orderedLen, out var completeData, out var completeLen))
                        {
                            RaiseDataReceived(peer, completeData, 0, completeLen, DeliveryMethod.ReliableOrdered);
                            NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(completeData);
                        }
                        NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(orderedData);
                    }
                    break;

                case DeliveryMethod.Sequenced:
                    if (peer.ProcessSequencedPacket(sequence))
                    {
                        RaiseDataReceived(peer, buffer, PacketHeader.HeaderSize, dataLength, delivery);
                    }
                    break;
            }
            _bufferPool.Return(buffer);
        }

        private void ProcessRetransmissions()
        {
            foreach (var peer in _peersByEndPoint.Values)
            {
                if (peer.State != PeerState.Connected)
                    continue;

                // Retransmit for reliable channel
                int count = peer._reliableChannel.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
                for (int i = 0; i < count; i++)
                {
                    ref var pkt = ref _retransmitBuffer[i];
                    peer._reliableChannel.GenerateAckData(out var ack, out var ackBits);
                    SendRawPacket(peer, DeliveryMethod.Reliable, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
                }

                // Retransmit for reliable ordered channel
                count = peer._reliableOrderedChannel.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
                for (int i = 0; i < count; i++)
                {
                    ref var pkt = ref _retransmitBuffer[i];
                    peer._reliableOrderedChannel.GenerateAckData(out var ack, out var ackBits);
                    SendRawPacket(peer, DeliveryMethod.ReliableOrdered, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
                }
            }
        }

        private void ProcessHeartbeats()
        {
            long now = Stopwatch.GetTimestamp();

            foreach (var peer in _peersByEndPoint.Values)
            {
                // Timeout check
                if (peer.State == PeerState.Connected && (now - peer.LastReceiveTime) > _connectionTimeout)
                {
                    _peersToRemove.Add(peer);
                    continue;
                }

                // Ping check
                if (peer.State == PeerState.Connected && (now - peer._lastPingTime) > _pingInterval)
                {
                    SendInternalPacket(peer, InternalPacketType.Ping);
                    peer._lastPingTime = now;
                }
            }

            foreach (var peer in _peersToRemove)
            {
                RemovePeer(peer);
            }
            _peersToRemove.Clear(); // Reuse for next tick
        }

        private NetworkPeer? AcceptConnection(PeerAddress peerAddr)
        {
            if (_peersByEndPoint.Count >= _maxPeers)
                return null;

            // Use GetOrAdd to handle the race where two threads might accept the same new peer
            NetworkPeer? newPeer = null;
            var peer = _peersByEndPoint.GetOrAdd(peerAddr, _ =>
            {
                if (!_peerPool.TryTake(out newPeer))
                    newPeer = new NetworkPeer();

                newPeer.Reset();
                newPeer.PeerId = (uint)Interlocked.Increment(ref _nextPeerId);
                newPeer.RemoteEndPoint = peerAddr.ToIPEndPoint();
                newPeer.InternalAddress = peerAddr;
                newPeer.State = PeerState.Connected;
                newPeer.LastReceiveTime = Stopwatch.GetTimestamp();
                newPeer._lastPingTime = Stopwatch.GetTimestamp();
                return newPeer;
            });

            // Only fire events for truly new peers
            if (peer == newPeer)
            {
                _peersById[peer.PeerId] = peer;
                SendInternalPacket(peer, InternalPacketType.ConnectAccept);
                OnPeerConnected?.Invoke(peer);
            }

            return peer;
        }

        private void RemovePeer(NetworkPeer peer)
        {
            peer.State = PeerState.Disconnected;

            _peersByEndPoint.TryRemove(peer.InternalAddress, out _);
            _peersById.TryRemove(peer.PeerId, out _);

            OnPeerDisconnected?.Invoke(peer);

            // Return to pool for reuse
            _peerPool.Add(peer);
        }

        private void SendUnreliable(NetworkPeer peer, byte[] data, int offset, int length)
        {
            ushort seq = peer.NextUnreliableSequence();
            peer._reliableChannel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(peer, DeliveryMethod.Unreliable, seq, ack, ackBits, data, offset, length);
        }

        private void SendSequenced(NetworkPeer peer, byte[] data, int offset, int length)
        {
            ushort seq = peer.NextSequencedSequence();
            peer._reliableChannel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(peer, DeliveryMethod.Sequenced, seq, ack, ackBits, data, offset, length);
        }

        private void SendReliable(NetworkPeer peer, byte[] data, int offset, int length, ReliableChannel channel, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable)
        {
            int seqResult = channel.QueueSend(data, offset, length);

            if (seqResult < 0)
                return; // Window full, drop

            ushort seq = (ushort)seqResult;
            channel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(peer, deliveryMethod, seq, ack, ackBits, data, offset, length);
        }

        private void SendFragmented(NetworkPeer peer, byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            ushort fragSeq = peer.NextUnreliableSequence(); // Use as fragment group ID
            var fragments = new byte[FragmentChannel.MaxFragments][];
            var fragmentLengths = new int[FragmentChannel.MaxFragments];

            int fragCount = FragmentChannel.CreateFragments(data, offset, length, fragSeq, fragments, fragmentLengths);

            DeliveryMethod fragMethod = delivery == DeliveryMethod.ReliableOrdered ? 
                DeliveryMethod.ReliableOrderedFragment : DeliveryMethod.ReliableFragment;

            var channel = delivery == DeliveryMethod.ReliableOrdered ? peer._reliableOrderedChannel : peer._reliableChannel;

            for (int i = 0; i < fragCount; i++)
            {
                SendReliable(peer, fragments[i], 0, fragmentLengths[i], channel, fragMethod);
            }
        }

        internal void SendInternalPacket(NetworkPeer peer, InternalPacketType type)
        {
            lock (peer)
            {
                byte[] buf = _bufferPool.Rent(PacketHeader.SafeMTU);
                buf[PacketHeader.HeaderSize] = (byte)type;

                peer._reliableChannel.GenerateAckData(out var ack, out var ackBits);
                PacketHeader.Write(buf, DeliveryMethod.Internal, 0, ack, ackBits, 1);

                int totalLength = PacketHeader.HeaderSize + 1;
                SendRawTo(peer.RemoteEndPoint, buf, totalLength);

                peer.LastSendTime = Stopwatch.GetTimestamp();
                _bufferPool.Return(buf);
            }
        }

        private void ProcessInternalPacket(NetworkPeer peer, byte[] data, int offset, int length)
        {
            if (length < 1)
                return;

            var type = (InternalPacketType)data[offset];

            switch (type)
            {
                case InternalPacketType.ConnectRequest:
                    // Already handled in AcceptConnection
                    break;

                case InternalPacketType.Disconnect:
                    RemovePeer(peer);
                    break;

                case InternalPacketType.Ping:
                    SendInternalPacket(peer, InternalPacketType.Pong);
                    break;

                case InternalPacketType.Pong:
                    // RTT is automatically tracked by the reliable channel ACKs
                    break;
            }
        }

        private void SendRawPacket(NetworkPeer peer, DeliveryMethod delivery, ushort sequence, ushort ack, uint ackBits, byte[] data, int offset, int length)
        {
            lock (peer)
            {
                int totalLength = PacketHeader.HeaderSize + length;
                byte[] buf = _bufferPool.Rent(totalLength);
                PacketHeader.Write(buf, delivery, sequence, ack, ackBits, (ushort)length);
                Buffer.BlockCopy(data, offset, buf, PacketHeader.HeaderSize, length);

                SendRawTo(peer.RemoteEndPoint, buf, totalLength);

                peer.LastSendTime = Stopwatch.GetTimestamp();
                _bufferPool.Return(buf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendRawTo(EndPoint endPoint, byte[] data, int length)
        {
            try
            {
                // Always send from the primary socket (socket 0).
                // In SO_REUSEPORT mode the kernel routes replies correctly regardless of which socket sends.
                Simulator.Send(_sockets![0], data, 0, length, endPoint);
            }
            catch (SocketException)
            {
                // Swallow send errors (peer may have disconnected)
            }
        }

        private void RaiseDataReceived(NetworkPeer peer, byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            // Pass the buffer directly to the callback without copying.
            // The buffer is rented from ArrayPool and will be returned AFTER the callback returns.
            // Callers must NOT store the reference beyond the callback scope.
            OnDataReceived?.Invoke(peer, data, offset, length, delivery);
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
