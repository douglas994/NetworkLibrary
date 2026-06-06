/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - NetworkClient
 *
 *  UDP client that connects to a NetworkServer.
 *  Handles connection handshake, reliability, and reconnection.
 */

using System;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Client connection state.
    /// </summary>
    public enum ClientState : byte
    {
        Disconnected = 0,
        Connecting = 1,
        Connected = 2,
        Disconnecting = 3
    }

    /// <summary>
    /// High-performance UDP client for connecting to a NetworkServer.
    /// </summary>
    public sealed class NetworkClient : IDisposable
    {
        // ── Configuration ──
        private readonly int _connectionTimeoutMs;
        private readonly int _maxConnectAttempts;

        // ── Socket ──
        private Socket? _socket;
        private IPEndPoint _serverEndPoint = new IPEndPoint(IPAddress.Any, 0);

        // ── Threading (I/O) ──
        private Thread? _receiveThread;
        private readonly NetworkLibrary.Threading.SpscQueue<RawPacket> _receiveQueue;
        private volatile bool _isRunning;

        // ── State ──
        public ClientState State { get; private set; }

        // ── Channels ──
        private readonly ReliableChannel _reliableChannel;
        private readonly ReliableChannel _reliableOrderedChannel;
        private readonly FragmentChannel _fragmentChannel;

        // ── Sequencing ──
        private ushort _unreliableLocalSequence;

        // ── Simulation (Chaos Monkey) ──
        public NetworkConditionSimulator Simulator { get; } = new NetworkConditionSimulator();

        private readonly object _sendLock = new object();
        private ushort _sequencedLocalSequence;
        private ushort _sequencedRemoteSequence;

        // ── Buffers ──
        private readonly System.Buffers.ArrayPool<byte> _bufferPool;
        private readonly byte[] _receiveBuffer;

        // ── Retransmit ──
        private readonly PendingPacket[] _retransmitBuffer;

        // ── Connection ──
        private long _connectTimestamp;
        private int _connectAttempts;
        private long _lastReceiveTime;
        private long _lastPingTime;
        private readonly long _connectionTimeout;
        private readonly long _pingInterval;

        // ── Events ──

        /// <summary>Called when successfully connected to server.</summary>
        public Action? OnConnected;

        /// <summary>Called when disconnected from server.</summary>
        public Action? OnDisconnected;

        /// <summary>Called when data is received from server.</summary>
        public Action<byte[], int, int, DeliveryMethod>? OnDataReceived;

        /// <summary>Estimated round-trip time in milliseconds.</summary>
        public float RTT => _reliableChannel.RTT;

        /// <summary>
        /// Application connection token. Must match the server's <see cref="NetworkServer.ConnectionKey"/>
        /// or the connection is rejected. Set before calling <see cref="Connect"/>. 0 = no token.
        /// </summary>
        public uint ConnectionKey { get; set; } = 0;

        /// <summary>
        /// Wire protocol version. Must match the server's <see cref="NetworkServer.ProtocolVersion"/>.
        /// Bump it whenever the packet format changes so old clients are rejected. Set before <see cref="Connect"/>.
        /// </summary>
        public ushort ProtocolVersion { get; set; } = 1;

        /// <summary>
        /// Creates a new NetworkClient.
        /// </summary>
        public NetworkClient(int connectionTimeoutMs = 15000, int maxConnectAttempts = 10, int pingIntervalMs = 1000)
        {
            _connectionTimeoutMs = connectionTimeoutMs;
            _maxConnectAttempts = maxConnectAttempts;
            _connectionTimeout = (long)(connectionTimeoutMs / 1000.0 * Stopwatch.Frequency);
            _pingInterval = (long)(pingIntervalMs / 1000.0 * Stopwatch.Frequency);

            _reliableChannel = new ReliableChannel();
            _reliableOrderedChannel = new ReliableChannel();
            _fragmentChannel = new FragmentChannel();

            _bufferPool = System.Buffers.ArrayPool<byte>.Shared;
            _receiveBuffer = new byte[PacketHeader.SafeMTU];
            _receiveQueue = new NetworkLibrary.Threading.SpscQueue<RawPacket>(1024); // 1k buffer is more than enough for a single client
            _retransmitBuffer = new PendingPacket[64];

            _serverEndPoint = new IPEndPoint(IPAddress.Any, 0);
            State = ClientState.Disconnected;
        }

        /// <summary>
        /// Initiates connection to a server.
        /// </summary>
        public void Connect(string host, int port)
        {
            if (State != ClientState.Disconnected)
                throw new InvalidOperationException("Already connected or connecting");

            var addresses = Dns.GetHostAddresses(host);
            if (addresses.Length == 0)
                throw new ArgumentException($"Could not resolve host: {host}");

            var address = addresses[0];
            _serverEndPoint = new IPEndPoint(address, port);

            _socket = new Socket(address.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            _socket.Blocking = true;
            _socket.ReceiveBufferSize = 256 * 1024;
            _socket.SendBufferSize = 256 * 1024;

            // Ignore ICMP port unreachable on Windows
            try
            {
                const int SIO_UDP_CONNRESET = unchecked((int)0x9800000C);
                _socket.IOControl(SIO_UDP_CONNRESET, new byte[] { 0 }, null);
            }
            catch { }

            _socket.Bind(new IPEndPoint(address.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0));

            State = ClientState.Connecting;
            _connectTimestamp = Stopwatch.GetTimestamp();
            _connectAttempts = 0;

            _isRunning = true;
            _receiveThread = new Thread(ReceiveLoop)
            {
                Name = "NetClient_Receive",
                IsBackground = true,
                Priority = ThreadPriority.AboveNormal
            };
            _receiveThread.Start();

            // Send initial connect request
            SendConnectRequest();
        }

        /// <summary>
        /// Disconnects from the server.
        /// </summary>
        public void Disconnect()
        {
            if (State == ClientState.Disconnected)
                return;
                
            if (State == ClientState.Connected)
            {
                SendInternalPacket(InternalPacketType.Disconnect);
            }

            CleanupConnection();
        }

        /// <summary>
        /// Processes network events. Must be called regularly from the game thread.
        /// </summary>
        public void Update()
        {
            if (_socket == null)
                return;

            Simulator.Tick(_socket!);

            // 1. Receive packets
            ReceivePackets();

            // 2. Handle connection state
            if (State == ClientState.Connecting)
            {
                ProcessConnecting();
            }
            else if (State == ClientState.Connected)
            {
                // Retransmissions
                ProcessRetransmissions();

                // Timeout check
                long now = Stopwatch.GetTimestamp();

                if ((now - _lastReceiveTime) > _connectionTimeout)
                {
                    CleanupConnection();
                    return;
                }

                // Ping
                if ((now - _lastPingTime) > _pingInterval)
                {
                    SendInternalPacket(InternalPacketType.Ping);
                    _lastPingTime = now;
                }
            }
        }

        /// <summary>
        /// Sends data to the server.
        /// </summary>
        public void Send(byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            if (State != ClientState.Connected)
                return;

            // Check fragmentation
            if (FragmentChannel.NeedsFragmentation(length) && delivery != DeliveryMethod.Unreliable)
            {
                SendFragmented(data, offset, length, delivery);
                return;
            }

            switch (delivery)
            {
                case DeliveryMethod.Unreliable:
                    SendUnreliable(data, offset, length);
                    break;

                case DeliveryMethod.Reliable:
                    SendReliable(data, offset, length, _reliableChannel, DeliveryMethod.Reliable);
                    break;

                case DeliveryMethod.ReliableOrdered:
                    SendReliable(data, offset, length, _reliableOrderedChannel, DeliveryMethod.ReliableOrdered);
                    break;

                case DeliveryMethod.Sequenced:
                    SendSequenced(data, offset, length);
                    break;
            }
        }

        // ════════════════════════════════════════════════════════
        //  PRIVATE IMPLEMENTATION
        // ════════════════════════════════════════════════════════

        private void ReceiveLoop()
        {
#if NETSTANDARD2_1
            EndPoint remoteSA = new IPEndPoint(_socket!.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
#else
            SocketAddress remoteSA = new SocketAddress(_socket!.AddressFamily);
#endif

            while (_isRunning && _socket != null)
            {
                int received;
                try
                {
#if NETSTANDARD2_1
                    received = _socket.ReceiveFrom(_receiveBuffer, 0, _receiveBuffer.Length, SocketFlags.None, ref remoteSA);
#else
                    received = _socket.ReceiveFrom(_receiveBuffer.AsSpan(), SocketFlags.None, remoteSA);
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
                Buffer.BlockCopy(_receiveBuffer, 0, dataCopy, 0, received);

                var packet = new RawPacket
                {
                    Buffer = dataCopy,
                    Length = received,
                    Sender = default // Not used in client
                };

                if (!_receiveQueue.TryEnqueue(packet))
                {
                    _bufferPool.Return(dataCopy); // Queue full
                }
            }
        }

        private void ReceivePackets()
        {
            while (_receiveQueue.TryDequeue(out var packet))
            {
                int received = packet.Length;
                byte[] data = packet.Buffer;

                if (!PacketHeader.Read(data, received, out var delivery, out var sequence, out var ack, 
out var ackBits, out var dataLength))
                {
                    _bufferPool.Return(data);
                    continue;
                }

                _lastReceiveTime = Stopwatch.GetTimestamp();

                if (delivery == DeliveryMethod.Internal)
                {
                    ProcessInternalPacket(data, PacketHeader.HeaderSize, dataLength);
                    _bufferPool.Return(data);
                    continue;
                }

                // If connecting, ignore non-internal
                if (State != ClientState.Connected)
                {
                    _bufferPool.Return(data);
                    continue;
                }

                ProcessReceive(data, received, dataLength, delivery, sequence, ack, ackBits, storeData: true);
            }
        }

        private void ProcessReceive(byte[] buffer, int totalReceived, int dataLength, DeliveryMethod delivery, 
ushort sequence, ushort ack, uint ackBits, bool storeData)
        {
            // Process ACKs
            if (delivery == DeliveryMethod.Reliable || delivery == DeliveryMethod.ReliableOrdered)
            {
                _reliableChannel.ProcessAck(ack, ackBits);
                _reliableOrderedChannel.ProcessAck(ack, ackBits);
            }

            int payloadLength = Math.Min(dataLength, totalReceived - PacketHeader.HeaderSize);

            switch (delivery)
            {
                case DeliveryMethod.Unreliable:
                    OnDataReceived?.Invoke(buffer, PacketHeader.HeaderSize, payloadLength, delivery);
                    _bufferPool.Return(buffer);
                    return;

                case DeliveryMethod.Reliable:
                    if (_reliableChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, payloadLength))
                    {
                        RaiseDataReceived(buffer, PacketHeader.HeaderSize, payloadLength, delivery);
                    }
                    break;

                case DeliveryMethod.ReliableOrdered:
                    _reliableOrderedChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, payloadLength, true);
                    while (_reliableOrderedChannel.TryGetNextOrdered(out var orderedData, out var orderedLen))
                    {
                        RaiseDataReceived(orderedData, 0, orderedLen, DeliveryMethod.ReliableOrdered);
                        NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(orderedData);
                    }
                    break;

                case DeliveryMethod.ReliableFragment:
                    if (_reliableChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, payloadLength))
                    {
                        if (_fragmentChannel.ProcessFragment(buffer, PacketHeader.HeaderSize, payloadLength, out var completeData, out var completeLen))
                        {
                            RaiseDataReceived(completeData, 0, completeLen, DeliveryMethod.Reliable);
                            NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(completeData);
                        }
                    }
                    break;

                case DeliveryMethod.ReliableOrderedFragment:
                    _reliableOrderedChannel.ProcessReceive(sequence, buffer, PacketHeader.HeaderSize, payloadLength, true);
                    while (_reliableOrderedChannel.TryGetNextOrdered(out var orderedData, out var orderedLen))
                    {
                        if (_fragmentChannel.ProcessFragment(orderedData, 0, orderedLen, out var completeData, out var completeLen))
                        {
                            RaiseDataReceived(completeData, 0, completeLen, DeliveryMethod.ReliableOrdered);
                            NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(completeData);
                        }
                        NetworkLibrary.Buffers.ArrayPool<byte>.Shared.Return(orderedData);
                    }
                    break;

                case DeliveryMethod.Sequenced:
                    if (ProcessSequencedPacket(sequence))
                    {
                        OnDataReceived?.Invoke(buffer, PacketHeader.HeaderSize, payloadLength, delivery);
                    }
                    _bufferPool.Return(buffer);
                    return;
            }
            _bufferPool.Return(buffer);
        }

        private void ProcessConnecting()
        {
            long now = Stopwatch.GetTimestamp();
            long elapsed = now - _connectTimestamp;

            // Retry connect every 500ms
            long retryInterval = Stopwatch.Frequency / 2;

            if (elapsed > retryInterval * (_connectAttempts + 1))
            {
                _connectAttempts++;

                if (_connectAttempts >= _maxConnectAttempts)
                {
                    CleanupConnection();
                    return;
                }

                SendConnectRequest();
            }
        }

        private void ProcessRetransmissions()
        {
            int count = _reliableChannel.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
            for (int i = 0; i < count; i++)
            {
                ref var pkt = ref _retransmitBuffer[i];
                _reliableChannel.GenerateAckData(out var ack, out var ackBits);
                SendRawPacket(DeliveryMethod.Reliable, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
            }

            count = _reliableOrderedChannel.GetPacketsToRetransmit(_retransmitBuffer, _retransmitBuffer.Length);
            for (int i = 0; i < count; i++)
            {
                ref var pkt = ref _retransmitBuffer[i];
                _reliableOrderedChannel.GenerateAckData(out var ack, out var ackBits);
                SendRawPacket(DeliveryMethod.ReliableOrdered, pkt.Sequence, ack, ackBits, pkt.Data, 0, pkt.DataLength);
            }
        }

        private void ProcessInternalPacket(byte[] data, int offset, int length)
        {
            if (length < 1)
                return;

            var type = (InternalPacketType)data[offset];

            switch (type)
            {
                case InternalPacketType.ConnectAccept:
                    if (State == ClientState.Connecting)
                    {
                        State = ClientState.Connected;
                        _lastReceiveTime = Stopwatch.GetTimestamp();
                        _lastPingTime = Stopwatch.GetTimestamp();
                        OnConnected?.Invoke();
                    }
                    break;

                case InternalPacketType.ConnectReject:
                    CleanupConnection();
                    break;

                case InternalPacketType.Disconnect:
                    CleanupConnection();
                    break;

                case InternalPacketType.Ping:
                    SendInternalPacket(InternalPacketType.Pong);
                    break;

                case InternalPacketType.Pong:
                    // RTT tracked via reliable channel ACKs
                    break;
            }
        }

        private void SendConnectRequest()
        {
            // ConnectRequest carries the handshake payload: [type][protocolVersion u16][connectionKey u32].
            // The server validates these before accepting the peer, rejecting spoof/garbage/incompatible clients.
            lock (_sendLock)
            {
                const int payloadLen = 1 + 2 + 4;
                byte[] buf = _bufferPool.Rent(PacketHeader.SafeMTU);
                int p = PacketHeader.HeaderSize;

                buf[p]     = (byte)InternalPacketType.ConnectRequest;
                buf[p + 1] = (byte)(ProtocolVersion & 0xFF);
                buf[p + 2] = (byte)(ProtocolVersion >> 8);
                buf[p + 3] = (byte)(ConnectionKey & 0xFF);
                buf[p + 4] = (byte)((ConnectionKey >> 8) & 0xFF);
                buf[p + 5] = (byte)((ConnectionKey >> 16) & 0xFF);
                buf[p + 6] = (byte)((ConnectionKey >> 24) & 0xFF);

                _reliableChannel.GenerateAckData(out var ack, out var ackBits);
                PacketHeader.Write(buf, DeliveryMethod.Internal, 0, ack, ackBits, payloadLen);

                SendRawTo(buf, PacketHeader.HeaderSize + payloadLen);
                _bufferPool.Return(buf);
            }
        }

        private void SendInternalPacket(InternalPacketType type)
        {
            lock (_sendLock)
            {
                byte[] buf = _bufferPool.Rent(PacketHeader.SafeMTU);
                buf[PacketHeader.HeaderSize] = (byte)type;

                _reliableChannel.GenerateAckData(out var ack, out var ackBits);
                PacketHeader.Write(buf, DeliveryMethod.Internal, 0, ack, ackBits, 1);

                int totalLength = PacketHeader.HeaderSize + 1;
                SendRawTo(buf, totalLength);
                _bufferPool.Return(buf);
            }
        }

        private void SendUnreliable(byte[] data, int offset, int length)
        {
            ushort seq = _unreliableLocalSequence++;
            _reliableChannel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(DeliveryMethod.Unreliable, seq, ack, ackBits, data, offset, length);
        }

        private void SendSequenced(byte[] data, int offset, int length)
        {
            ushort seq = _sequencedLocalSequence++;
            _reliableChannel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(DeliveryMethod.Sequenced, seq, ack, ackBits, data, offset, length);
        }

        private void SendReliable(byte[] data, int offset, int length, ReliableChannel channel, DeliveryMethod deliveryMethod = DeliveryMethod.Reliable)
        {
            int seqResult = channel.QueueSend(data, offset, length);

            if (seqResult < 0)
                return; // Window full, drop

            ushort seq = (ushort)seqResult;
            channel.GenerateAckData(out var ack, out var ackBits);
            SendRawPacket(deliveryMethod, seq, ack, ackBits, data, offset, length);
        }

        private void SendFragmented(byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            ushort fragSeq = _unreliableLocalSequence++; // Use as fragment group ID
            var fragments = new byte[FragmentChannel.MaxFragments][];
            var fragmentLengths = new int[FragmentChannel.MaxFragments];

            int fragCount = FragmentChannel.CreateFragments(data, offset, length, fragSeq, fragments, fragmentLengths);

            DeliveryMethod fragMethod = delivery == DeliveryMethod.ReliableOrdered ? 
                DeliveryMethod.ReliableOrderedFragment : DeliveryMethod.ReliableFragment;

            var channel = delivery == DeliveryMethod.ReliableOrdered ? _reliableOrderedChannel : _reliableChannel;

            for (int i = 0; i < fragCount; i++)
            {
                SendReliable(fragments[i], 0, fragmentLengths[i], channel, fragMethod);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ProcessSequencedPacket(ushort sequence)
        {
            if (PacketHeader.IsSequenceNewer(sequence, _sequencedRemoteSequence))
            {
                _sequencedRemoteSequence = sequence;
                return true;
            }
            return false;
        }

        private void SendRawPacket(DeliveryMethod delivery, ushort sequence, ushort ack, uint ackBits, byte[] data, int offset, int length)
        {
            lock (_sendLock)
            {
                int totalLength = PacketHeader.HeaderSize + length;
                byte[] buf = _bufferPool.Rent(totalLength);
                PacketHeader.Write(buf, delivery, sequence, ack, ackBits, (ushort)length);
                Buffer.BlockCopy(data, offset, buf, PacketHeader.HeaderSize, length);

                SendRawTo(buf, totalLength);
                _bufferPool.Return(buf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendRawTo(byte[] data, int length)
        {
            try
            {
                Simulator.Send(_socket!, data, 0, length, _serverEndPoint);
            }
            catch (SocketException) { }
        }

        private void RaiseDataReceived(byte[] data, int offset, int length, DeliveryMethod delivery)
        {
            // Pass the buffer directly to the callback without copying.
            // The buffer is rented from ArrayPool and will be returned AFTER the callback returns.
            // Callers must NOT store the reference beyond the callback scope.
            OnDataReceived?.Invoke(data, offset, length, delivery);
        }

        private void CleanupConnection()
        {
            var wasConnected = State == ClientState.Connected;

            State = ClientState.Disconnected;
            _socket?.Close();
            _socket = null;

            _reliableChannel.Reset();
            _reliableOrderedChannel.Reset();
            _unreliableLocalSequence = 0;
            _sequencedLocalSequence = 0;
            _sequencedRemoteSequence = 0;

            if (wasConnected)
                OnDisconnected?.Invoke();
        }

        public void Dispose()
        {
            Disconnect();
        }
    }
}
