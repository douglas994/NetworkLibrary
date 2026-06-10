/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - NetworkServer
 *
 *  UDP server that manages multiple client connections.
 *  Uses raw Socket with SocketAsyncEventArgs for zero-allocation async I/O.
 *  No connection limit — scales to thousands of concurrent peers.
 */

using System;
using System.Runtime.InteropServices;
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
        /// Enables parallel packet reception with <paramref name="workerCount"/> receive threads.
        /// <list type="bullet">
        /// <item>On Linux: spawns <paramref name="workerCount"/> sockets with SO_REUSEPORT, so the
        /// kernel load-balances datagrams by 5-tuple hash (each peer always lands on the same socket).</item>
        /// <item>On Windows (no SO_REUSEPORT load-balancing): spawns <paramref name="workerCount"/> threads
        /// that all call ReceiveFrom on a single shared socket. The OS hands each datagram to one waiting
        /// thread, which still scales receive throughput past a single thread's syscall ceiling.</item>
        /// </list>
        /// Must be called BEFORE <see cref="Start"/>.
        /// </summary>
        /// <param name="workerCount">Number of receive threads. Recommended: Environment.ProcessorCount / 2.</param>
        public void EnableReusePort(int workerCount)
        {
            if (_isRunning)
                throw new InvalidOperationException("Cannot enable ReusePort after Start()");

            _reusePortWorkers = Math.Max(1, workerCount);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                Console.WriteLine($"[NetworkServer] SO_REUSEPORT enabled: {_reusePortWorkers} load-balanced sockets.");
            else
                Console.WriteLine($"[NetworkServer] Multi-threaded receive: {_reusePortWorkers} threads on 1 shared socket.");
        }

        /// <summary>
        /// Starts listening for connections on the specified port.
        /// </summary>
        public void Start(int port, bool ipv6 = false)
        {
            if (_isRunning)
                throw new InvalidOperationException("Server is already running");

            int workerCount = Math.Max(1, _reusePortWorkers);

            // Linux: one SO_REUSEPORT socket per thread (kernel load-balances).
            // Windows: one shared socket, multiple threads calling ReceiveFrom on it.
            bool useReusePort = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && workerCount > 1;

            // SO_REUSEPORT is best-effort: some environments (e.g. Docker Desktop's Linux VM) reject it with
            // EOPNOTSUPP. Probe once and fall back to a single shared socket + multi-thread receive if unsupported.
            if (useReusePort && !SupportsReusePort())
            {
                Console.WriteLine("[NetworkServer] SO_REUSEPORT unsupported here → single shared socket + multi-thread receive.");
                useReusePort = false;
            }

            int socketCount = useReusePort ? workerCount : 1;

            _sockets = new Socket[socketCount];
            _receiveThreads = new Thread[workerCount];

            for (int i = 0; i < socketCount; i++)
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
                if (useReusePort)
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
            }

            // Spawn receive threads. On Linux each thread owns its socket; on Windows all
            // threads share socket[0] (the OS distributes datagrams across waiting threads).
            for (int t = 0; t < workerCount; t++)
            {
                int socketIndex = useReusePort ? t : 0;
                var thread = new Thread(() => ReceiveLoop(socketIndex))
                {
                    Name = $"NetServer_Recv_{t}",
                    IsBackground = true,
                    Priority = ThreadPriority.Highest
                };
                _receiveThreads[t] = thread;
            }

            _isRunning = true;

            // Start all receive threads after all sockets are bound
            for (int t = 0; t < workerCount; t++)
                _receiveThreads[t].Start();
        }

        /// <summary>Probes whether SO_REUSEPORT can be set on a UDP socket here (best-effort; some kernels/containers
        /// reject it). Done on a throwaway socket so <see cref="Start"/> can choose single-socket mode safely.</summary>
        private static bool SupportsReusePort()
        {
            try
            {
                using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                probe.SetSocketOption(SocketOptionLevel.Socket, (SocketOptionName)15, true); // 15 = SO_REUSEPORT
                return true;
            }
            catch
            {
                return false;
            }
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
        /// <remarks>
        /// NOT thread-safe per peer: do not call Send for the SAME peer from multiple threads
        /// concurrently (the reliable channel's sequence/window is unlocked by design). Sending to
        /// DIFFERENT peers in parallel is safe. See <see cref="NetworkLibrary.NetPeer"/> remarks.
        /// </remarks>
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
            foreach (var kvp in _peersByEndPoint)
            {
                var peer = kvp.Value;
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
            foreach (var kvp in _peersByEndPoint)
            {
                var peer = kvp.Value;
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
        // ── Handshake / security ──

        /// <summary>
        /// Application connection token. A client's <see cref="NetworkClient.ConnectionKey"/> must match
        /// this exact value or its connection is rejected. 0 = no token (open). Set a non-zero secret
        /// shared with your client to block spoofed/garbage connections from random scanners.
        /// </summary>
        public uint ConnectionKey { get; set; } = 0;

        /// <summary>
        /// Wire protocol version. A client's <see cref="NetworkClient.ProtocolVersion"/> must match or it
        /// is rejected. Bump it when the packet format changes so outdated clients can't connect.
        /// </summary>
        public ushort ProtocolVersion { get; set; } = 1;

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
            // Match the socket's family so ReceiveFrom can write the sender address.
#if NETSTANDARD2_1
            EndPoint remoteSA = new IPEndPoint(socket.AddressFamily == AddressFamily.InterNetworkV6 ? System.Net.IPAddress.IPv6Any : System.Net.IPAddress.Any, 0);
#else
            SocketAddress remoteSA = new SocketAddress(socket.AddressFamily);
#endif

            while (_isRunning)
            {
                int received;
                try
                {
#if NETSTANDARD2_1
                    received = socket.ReceiveFrom(receiveBuffer, 0, receiveBuffer.Length, SocketFlags.None, ref remoteSA);
#else
                    received = socket.ReceiveFrom(receiveBuffer.AsSpan(), SocketFlags.None, remoteSA);
#endif
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
#if NETSTANDARD2_1
                    Sender = new PeerAddress((IPEndPoint)remoteSA)
#else
                    Sender = new PeerAddress(remoteSA)
#endif
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
                    // New connection request — only accept a valid ConnectRequest (right protocol
                    // version + connection token). Everything else from an unknown peer is dropped,
                    // so spoofed/garbage/incompatible packets never create a peer.
                    if (delivery == DeliveryMethod.Internal && IsValidConnectRequest(data, received, dataLength))
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
            // Iterate the dictionary directly (struct enumerator, no per-tick snapshot
            // allocation — unlike .Values which copies every value into a new List).
            foreach (var kvp in _peersByEndPoint)
            {
                var peer = kvp.Value;
                if (peer.State != PeerState.Connected)
                    continue;

                var reliable = peer._reliableChannel;
                var ordered = peer._reliableOrderedChannel;

                // Fast path: skip peers with nothing in flight on either channel.
                if (!reliable.HasUnackedPackets && !ordered.HasUnackedPackets)
                    continue;

                // Retransmit for reliable channel
                if (reliable.HasUnackedPackets)
                {
                    int count = reliable.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
                    if (count > 0)
                    {
                        // ACK state is identical for every packet in this batch — compute once.
                        reliable.GenerateAckData(out var ack, out var ackBits);
                        for (int i = 0; i < count; i++)
                        {
                            ref var pkt = ref _retransmitBuffer[i];
                            SendRawPacket(peer, DeliveryMethod.Reliable, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
                        }
                    }
                }

                // Retransmit for reliable ordered channel
                if (ordered.HasUnackedPackets)
                {
                    int count = ordered.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
                    if (count > 0)
                    {
                        ordered.GenerateAckData(out var ack, out var ackBits);
                        for (int i = 0; i < count; i++)
                        {
                            ref var pkt = ref _retransmitBuffer[i];
                            SendRawPacket(peer, DeliveryMethod.ReliableOrdered, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
                        }
                    }
                }
            }
        }

        private void ProcessHeartbeats()
        {
            long now = Stopwatch.GetTimestamp();

            foreach (var kvp in _peersByEndPoint)
            {
                var peer = kvp.Value;
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

        /// <summary>
        /// Validates a handshake packet from an unknown peer: it must be a ConnectRequest carrying
        /// a matching protocol version and connection token. Rejects everything else.
        /// Payload layout (after the 12-byte header): [type:1][protocolVersion:2][connectionKey:4].
        /// </summary>
        private bool IsValidConnectRequest(byte[] data, int received, int dataLength)
        {
            int p = PacketHeader.HeaderSize;
            const int connectPayload = 1 + 2 + 4;

            if (dataLength < connectPayload || received < p + connectPayload)
                return false;

            if ((InternalPacketType)data[p] != InternalPacketType.ConnectRequest)
                return false;

            ushort version = (ushort)(data[p + 1] | (data[p + 2] << 8));
            uint key = (uint)(data[p + 3] | (data[p + 4] << 8) | (data[p + 5] << 16) | (data[p + 6] << 24));

            return version == ProtocolVersion && key == ConnectionKey;
        }

        private NetworkPeer? AcceptConnection(PeerAddress peerAddr)
        {
            if (_peersByEndPoint.Count >= _maxPeers)
                return null;

            // Fast path: already known peer, no allocation, no candidate setup.
            if (_peersByEndPoint.TryGetValue(peerAddr, out var existing))
                return existing;

            // Prepare a candidate peer (pooled). Using the GetOrAdd(key, value) overload
            // instead of GetOrAdd(key, factory) avoids allocating a closure on every
            // connection attempt. If we lose the race, the candidate is returned to the pool.
            if (!_peerPool.TryTake(out var candidate))
                candidate = new NetworkPeer();

            candidate.Reset();
            candidate.PeerId = (uint)Interlocked.Increment(ref _nextPeerId);
            candidate.RemoteEndPoint = peerAddr.ToIPEndPoint();
            candidate._sendAddress = candidate.RemoteEndPoint.Serialize(); // cache for zero-alloc sends
            candidate.InternalAddress = peerAddr;
            candidate.State = PeerState.Connected;
            candidate.LastReceiveTime = Stopwatch.GetTimestamp();
            candidate._lastPingTime = Stopwatch.GetTimestamp();

            var peer = _peersByEndPoint.GetOrAdd(peerAddr, candidate);

            if (peer == candidate)
            {
                // We won the race — register and fire events.
                _peersById[peer.PeerId] = peer;
                SendInternalPacket(peer, InternalPacketType.ConnectAccept);
                OnPeerConnected?.Invoke(peer);
            }
            else
            {
                // Another thread added this peer first — recycle our unused candidate.
                _peerPool.Add(candidate);
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
            // Stack buffer — thread-local, no pool, no lock needed.
            const int totalLength = PacketHeader.HeaderSize + 1;
            Span<byte> buf = stackalloc byte[totalLength];

            peer._reliableChannel.GenerateAckData(out var ack, out var ackBits);
            PacketHeader.Write(buf, DeliveryMethod.Internal, 0, ack, ackBits, 1);
            buf[PacketHeader.HeaderSize] = (byte)type;

            SendRawTo(peer, buf);
            peer.LastSendTime = Stopwatch.GetTimestamp();
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
            int totalLength = PacketHeader.HeaderSize + length;

            // Common case (fits in one datagram): build the packet on the stack — no lock,
            // no pool rent/return, so concurrent broadcast threads never contend on the pool.
            if (totalLength <= PacketHeader.SafeMTU)
            {
                Span<byte> packet = stackalloc byte[totalLength];
                PacketHeader.Write(packet, delivery, sequence, ack, ackBits, (ushort)length);
                data.AsSpan(offset, length).CopyTo(packet.Slice(PacketHeader.HeaderSize));
                SendRawTo(peer, packet);
            }
            else
            {
                // Oversized (e.g. large Unreliable that bypasses fragmentation) — fall back to the pool.
                byte[] rented = _bufferPool.Rent(totalLength);
                PacketHeader.Write(rented, delivery, sequence, ack, ackBits, (ushort)length);
                Buffer.BlockCopy(data, offset, rented, PacketHeader.HeaderSize, length);
                SendRawTo(peer, rented.AsSpan(0, totalLength));
                _bufferPool.Return(rented);
            }

            peer.LastSendTime = Stopwatch.GetTimestamp();
        }

#if NETSTANDARD2_1
        private void SendRawTo(NetworkPeer peer, ReadOnlySpan<byte> data)
        {
            try
            {
                byte[] tmp = _bufferPool.Rent(data.Length);
                data.CopyTo(tmp);

                if (Simulator.Enabled)
                {
                    Simulator.Send(_sockets![0], tmp, 0, data.Length, peer.RemoteEndPoint);
                }
                else
                {
                    _sockets![0].SendTo(tmp, 0, data.Length, SocketFlags.None, peer.RemoteEndPoint);
                }

                _bufferPool.Return(tmp);
            }
            catch (SocketException)
            {
                // Swallow send errors (peer may have disconnected)
            }
        }
#else
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendRawTo(NetworkPeer peer, ReadOnlySpan<byte> data)
        {
            try
            {
                // Always send from the primary socket (socket 0).
                // In SO_REUSEPORT mode the kernel routes replies correctly regardless of which socket sends.
                if (Simulator.Enabled)
                {
                    // The simulator's delay queue needs a byte[]; rent a temporary copy.
                    byte[] tmp = _bufferPool.Rent(data.Length);
                    data.CopyTo(tmp);
                    Simulator.Send(_sockets![0], tmp, 0, data.Length, peer.RemoteEndPoint);
                    _bufferPool.Return(tmp);
                }
                else
                {
                    // Use the cached, pre-serialized SocketAddress — avoids allocating and
                    // serializing an IPEndPoint on every single send.
                    _sockets![0].SendTo(data, SocketFlags.None, peer._sendAddress!);
                }
            }
            catch (SocketException)
            {
                // Swallow send errors (peer may have disconnected)
            }
        }
#endif

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
