/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Core API - NetManager
 *
 *  The centralized Manager that abstracts UDP and TCP.
 *  Uses Pluggable Transports depending on initialization.
 */

using System;
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
    public sealed class NetManager : IDisposable
    {
        private readonly INetEventListener _listener;
        private readonly TransportType _transportType;

        // Server Backends
        private NetworkServer? _udpServer;
        private TcpServer? _tcpServer;

        // Client Backends
        private NetworkClient? _udpClient;
        private TcpClient? _tcpClient;
        private NetPeer? _clientPeer;

        /// <summary>
        /// Gets the network condition simulator if using UDP. Returns null for TCP.
        /// </summary>
        public NetworkConditionSimulator? Simulator => _udpServer?.Simulator ?? _udpClient?.Simulator;

        public PacketDispatcher Packets { get; } = new PacketDispatcher();

        [ThreadStatic]
        private static BitBuffer _receiveBuffer;

        // Peer Mappings
        // No concurrent access needed: all peer events fire from within Update() (single-threaded).
        private readonly Dictionary<uint, NetPeer> _peers;

        /// <summary>True if the server is listening or the client is connected.</summary>
        public bool IsRunning { get; private set; }

        public NetManager(INetEventListener listener, TransportType transportType)
        {
            _listener = listener;
            _transportType = transportType;
            _peers = new Dictionary<uint, NetPeer>();
        }

        // ════════════════════════════════════════════════════════
        // SERVER API
        // ════════════════════════════════════════════════════════

        /// <summary>
        /// Starts the NetManager in Server mode on the specified port.
        /// </summary>
        /// <param name="port">UDP/TCP port to listen on.</param>
        /// <param name="receiveThreads">
        /// Number of parallel packet-receive threads (UDP only). 1 = single thread.
        /// On Linux uses SO_REUSEPORT sockets; on Windows uses multiple threads on one shared socket.
        /// Recommended for high load: Environment.ProcessorCount / 2.
        /// </param>
        public void Start(int port, int receiveThreads = 1)
        {
            if (IsRunning) throw new InvalidOperationException("Manager is already running");
            IsRunning = true;

            if (_transportType == TransportType.Udp)
            {
                _udpServer = new NetworkServer();
                if (receiveThreads > 1)
                    _udpServer.EnableReusePort(receiveThreads);

                _udpServer.OnPeerConnected = (p) =>
                {
                    var netPeer = new NetPeer(p.PeerId,
                        (data, offset, len, method) => _udpServer.Send(p, data, offset, len, method),
                        () => _udpServer.DisconnectPeer(p));
                    
                    _peers[p.PeerId] = netPeer;
                    _listener.OnPeerConnected(netPeer);
                };

                _udpServer.OnPeerDisconnected = (p) =>
                {
                    if (_peers.Remove(p.PeerId, out var netPeer))
                    {
                        _listener.OnPeerDisconnected(netPeer, DisconnectReason.PeerDisconnected);
                    }
                };

                _udpServer.OnDataReceived = (p, data, offset, len, method) =>
                {
                    if (_peers.TryGetValue(p.PeerId, out var netPeer))
                    {
                        using var buffer = new BitBuffer();
                        buffer.FromSpan(new ReadOnlySpan<byte>(data, offset, len));
                        _listener.OnNetworkReceive(netPeer, buffer, method);

                        _receiveBuffer.Clear();
                        _receiveBuffer.FromSpan(new ReadOnlySpan<byte>(data, offset, len));
                        Packets.Dispatch(netPeer, ref _receiveBuffer);
                    }
                };

                _udpServer.Start(port);
            }
            else // TCP
            {
                _tcpServer = new TcpServer();

                _tcpServer.OnPeerConnected = (p) =>
                {
                    var netPeer = new NetPeer(p.PeerId,
                        (data, offset, len, method) => p.Send(data, offset, len), // TCP inherently ignores DeliveryMethod
                        () => p.Disconnect());

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
        }

        // ════════════════════════════════════════════════════════
        // CLIENT API
        // ════════════════════════════════════════════════════════

        /// <summary>
        /// Starts the NetManager in Client mode and connects to the server.
        /// </summary>
        public void Connect(string address, int port)
        {
            if (IsRunning) throw new InvalidOperationException("Manager is already running");
            IsRunning = true;

            if (_transportType == TransportType.Udp)
            {
                _udpClient = new NetworkClient();

                _udpClient.OnConnected = () =>
                {
                    _clientPeer = new NetPeer(0,
                        (data, offset, len, method) => _udpClient.Send(data, offset, len, method),
                        () => _udpClient.Disconnect());
                    
                    _listener.OnPeerConnected(_clientPeer);
                };

                _udpClient.OnDisconnected = () =>
                {
                    if (_clientPeer != null)
                    {
                        _listener.OnPeerDisconnected(_clientPeer, DisconnectReason.PeerDisconnected);
                        _clientPeer = null;
                    }
                };

                _udpClient.OnDataReceived = (data, offset, len, method) =>
                {
                    if (_clientPeer != null)
                    {
                        using var buffer = new BitBuffer();
                        buffer.FromSpan(new ReadOnlySpan<byte>(data, offset, len));
                        _listener.OnNetworkReceive(_clientPeer, buffer, method);

                        _receiveBuffer.Clear();
                        _receiveBuffer.FromSpan(new ReadOnlySpan<byte>(data, offset, len));
                        Packets.Dispatch(_clientPeer, ref _receiveBuffer);
                    }
                };

                _udpClient.Connect(address, port);
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
            _udpServer?.Update();
            _tcpServer?.Update();
            
            _udpClient?.Update();
            _tcpClient?.Update();
        }

        /// <summary>
        /// Stops the server or disconnects the client.
        /// </summary>
        public void Stop()
        {
            IsRunning = false;
            
            _udpServer?.Stop();
            _tcpServer?.Stop();

            _udpClient?.Disconnect();
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
