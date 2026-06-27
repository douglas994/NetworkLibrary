/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Core API - NetNode
 *
 *  The centralized Manager that abstracts UDP and TCP.
 *  Uses Pluggable Transports depending on initialization.
 */

using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using NetworkLibrary.Packets;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary
{
    /// <summary>
    /// The Centralized Network Manager. 
    /// Manages connections, routes events to the listener, and encapsulates the UDP/TCP complexity.
    /// </summary>
    public sealed class NetNode : IDisposable
    {
        private readonly INetEventListener _listener;
        private readonly TransportType _transportType;

        // UDP Backend (client + server) — LiteNetLib under the hood.
        private LiteNetUdpTransport? _udp;

        // TCP Backends
        private TcpServer? _tcpServer;
        private TcpClient? _tcpClient;
        private NetPeer? _clientPeer;

        /// <summary>
        /// Gets the network condition simulator if using UDP. Returns null for TCP.
        /// </summary>
        public NetworkConditionSimulator? Simulator => _udp?.Simulator;

        public PacketDispatcher Packets { get; } = new PacketDispatcher();

        /// <summary>
        /// Application connection token (UDP). Set the SAME value on client and server; a mismatch
        /// rejects the connection. 0 = no token. Set before Start/Connect. Blocks spoofed/garbage connects.
        /// </summary>
        public uint ConnectionKey { get; set; } = 0;

        /// <summary>
        /// Wire protocol version (UDP). Must match between client and server. Bump on packet-format changes.
        /// Set before Start/Connect.
        /// </summary>
        public ushort ProtocolVersion { get; set; } = 1;

        [ThreadStatic]
        private static BitBuffer _receiveBuffer;

        // Peer Mappings
        // No concurrent access needed: all peer events fire from within Update() (single-threaded).
        private readonly Dictionary<uint, NetPeer> _peers;

        /// <summary>True if the server is listening or the client is connected.</summary>
        public bool IsRunning { get; private set; }

        public NetNode(INetEventListener listener, TransportType transportType)
        {
            _listener = listener;
            _transportType = transportType;
            _peers = new Dictionary<uint, NetPeer>();
        }

        // ════════════════════════════════════════════════════════
        // SERVER API
        // ════════════════════════════════════════════════════════

        /// <summary>
        /// Starts the NetNode in Server mode on the specified port.
        /// </summary>
        /// <param name="port">UDP/TCP port to listen on.</param>
        /// <param name="receiveThreads">
        /// Number of parallel packet-receive threads (UDP only).
        /// If 0 (default), it automatically detects the OS and uses Environment.ProcessorCount / 2 on Linux for SO_REUSEPORT, or 1 on Windows.
        /// </param>
        public void Start(int port, int receiveThreads = 0)
        {
            if (IsRunning) throw new InvalidOperationException("Manager is already running");

            // Auto-detect optimal thread count for the current OS.
            // Capped at 4: receive scales sub-linearly (1 thread already handles ~600k pkt/s),
            // so we leave the rest of the cores for the game simulation.
            if (receiveThreads <= 0)
            {
                receiveThreads = RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                    ? Math.Clamp(Environment.ProcessorCount / 2, 1, 4)
                    : 1;
            }

            try
            {

            if (_transportType == TransportType.Udp)
            {
                // UDP reliability/ordering/fragmentation/handshake are handled by LiteNetLib inside
                // LiteNetUdpTransport, which maps each peer to our NetPeer and raises this listener.
                _udp = new LiteNetUdpTransport(_listener, Packets, ConnectionKey, ProtocolVersion, isServer: true);
                _udp.StartServer(port);
            }
            else // TCP
            {
                _tcpServer = new TcpServer();

                _tcpServer.OnPeerConnected = (p) =>
                {
                    var netPeer = new NetPeer(p.PeerId,
                        (data, offset, len, method) => p.Send(data, offset, len), // TCP inherently ignores DeliveryMethod
                        () => p.Disconnect(),
                        p.RemoteEndPoint);

                    _peers[p.PeerId] = netPeer;
                    _listener.OnPeerConnected(netPeer);
                };

                _tcpServer.OnPeerDisconnected = (p) =>
                {
                    if (_peers.Remove(p.PeerId, out var netPeer))
                    {
                        _listener.OnPeerDisconnected(netPeer, DisconnectReason.PeerDisconnected);
                    }
                };

                _tcpServer.OnDataReceived = (p, data, offset, length) =>
                {
                    if (_peers.TryGetValue(p.PeerId, out var netPeer))
                    {
                        using var buffer = new BitBuffer();
                        buffer.FromSpan(new ReadOnlySpan<byte>(data, offset, length));
                        _listener.OnNetworkReceive(netPeer, buffer, DeliveryMethod.ReliableOrdered);

                        _receiveBuffer.Clear();
                        _receiveBuffer.FromSpan(new ReadOnlySpan<byte>(data, offset, length));
                        Packets.Dispatch(netPeer, ref _receiveBuffer);
                    }
                };

                _tcpServer.Start(port);
            }

            // Only mark as running once everything bound/started successfully.
            IsRunning = true;
            }
            catch
            {
                // Bind failed (e.g. port in use) — roll back so Start can be retried.
                _udp?.Stop();
                _tcpServer?.Stop();
                _udp = null;
                _tcpServer = null;
                IsRunning = false;
                throw;
            }
        }

        // ════════════════════════════════════════════════════════
        // CLIENT API
        // ════════════════════════════════════════════════════════

        /// <summary>
        /// Starts the NetNode in Client mode and connects to the server.
        /// </summary>
        public void Connect(string address, int port)
        {
            if (IsRunning) throw new InvalidOperationException("Manager is already running");
            IsRunning = true;

            if (_transportType == TransportType.Udp)
            {
                _udp = new LiteNetUdpTransport(_listener, Packets, ConnectionKey, ProtocolVersion, isServer: false);
                _udp.Connect(address, port);
            }
            else // TCP
            {
                _tcpClient = new TcpClient();

                _tcpClient.OnConnected = () =>
                {
                    _clientPeer = new NetPeer(0,
                        (data, offset, len, method) => _tcpClient.Send(data, offset, len),
                        () => _tcpClient.Disconnect());

                    _listener.OnPeerConnected(_clientPeer);
                };

                _tcpClient.OnDisconnected = () =>
                {
                    if (_clientPeer != null)
                    {
                        _listener.OnPeerDisconnected(_clientPeer, DisconnectReason.PeerDisconnected);
                        _clientPeer = null;
                    }
                };

                _tcpClient.OnDataReceived = (data, offset, length) =>
                {
                    if (_clientPeer != null)
                    {
                        using var buffer = new BitBuffer();
                        buffer.FromSpan(new ReadOnlySpan<byte>(data, offset, length));
                        _listener.OnNetworkReceive(_clientPeer, buffer, DeliveryMethod.ReliableOrdered);

                        _receiveBuffer.Clear();
                        _receiveBuffer.FromSpan(new ReadOnlySpan<byte>(data, offset, length));
                        Packets.Dispatch(_clientPeer, ref _receiveBuffer);
                    }
                };

                _tcpClient.Connect(address, port);
            }
        }

        // ════════════════════════════════════════════════════════
        // LIFECYCLE
        // ════════════════════════════════════════════════════════

        /// <summary>
        /// Call this once per frame (e.g. Unity Update) to process all network events on the Main Thread.
        /// </summary>
        public void PollEvents()
        {
            _udp?.Poll();

            _tcpServer?.Update();
            _tcpClient?.Update();
        }

        /// <summary>
        /// Stops the server or disconnects the client.
        /// </summary>
        public void Stop()
        {
            IsRunning = false;

            _udp?.Stop();
            _tcpServer?.Stop();
            _tcpClient?.Disconnect();

            _peers.Clear();
            _clientPeer = null;
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
