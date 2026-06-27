/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - LiteNetUdpTransport
 *
 *  UDP transport backed by LiteNetLib (https://github.com/RevenantX/LiteNetLib).
 *  Replaces the custom reliable-UDP stack (ReliableChannel/FragmentChannel/NetworkServer/NetworkClient),
 *  which had reliability bugs under packet reordering. LiteNetLib owns reliability, ordering,
 *  fragmentation and the connection handshake; we keep BitBuffer, the packet layer, and our
 *  transport-agnostic NetPeer/INetEventListener abstraction on top.
 *
 *  Single-responsibility: this file is the ONLY place that touches LiteNetLib. The rest of the
 *  codebase keeps talking to NetManager/NetPeer/DeliveryMethod as before.
 */

using System;
using System.Net;
using System.Net.Sockets;
using NetworkLibrary.Packets;
using NetworkLibrary.Serialization;
using LNL = LiteNetLib;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Drives a LiteNetLib <c>NetManager</c> under our public API. Maps each LiteNetLib peer to one of our
    /// <see cref="NetPeer"/> (stored on <c>litePeer.Tag</c>) and raises our <see cref="INetEventListener"/> +
    /// <see cref="PacketDispatcher"/>, identically to how the old UDP backend did.
    /// </summary>
    internal sealed class LiteNetUdpTransport : LNL.INetEventListener
    {
        private readonly INetEventListener _app;
        private readonly PacketDispatcher _packets;
        private readonly string _key;          // "{protocolVersion}:{connectionKey}" — handshake gate
        private readonly bool _isServer;
        private readonly LNL.NetManager _net;

        private NetPeer? _clientPeer;           // client mode only

        // Second-pass buffer for PacketDispatcher (events fire single-threaded from PollEvents()).
        [ThreadStatic] private static BitBuffer _dispatchBuffer;

        /// <summary>Network-condition simulator (loss/latency/jitter). NOTE: LiteNetLib only honors this in
        /// DEBUG builds or when SIMULATE_NETWORK is defined — the NuGet (Release) DLL ignores it.</summary>
        public NetworkConditionSimulator Simulator { get; } = new NetworkConditionSimulator();

        public LiteNetUdpTransport(INetEventListener app, PacketDispatcher packets, uint connectionKey, ushort protocolVersion, bool isServer)
        {
            _app = app;
            _packets = packets;
            _key = $"{protocolVersion}:{connectionKey}";
            _isServer = isServer;
            _net = new LNL.NetManager(this)
            {
                UnsyncedEvents = false,    // callbacks only inside PollEvents() → preserves our single-thread contract
                IPv6Enabled = false,
                DisconnectTimeout = 15000,
            };
        }

        public void StartServer(int port) => _net.Start(port);

        public void Connect(string host, int port)
        {
            _net.Start();
            _net.Connect(host, port, _key);
        }

        public void Poll()
        {
            // Push simulator settings (no-op unless LiteNetLib was built with DEBUG/SIMULATE_NETWORK).
            var s = Simulator;
            _net.SimulatePacketLoss = s.Enabled && s.PacketLossPercent > 0f;
            _net.SimulationPacketLossChance = (int)s.PacketLossPercent;
            _net.SimulateLatency = s.Enabled && s.LatencyMs > 0;
            _net.SimulationMinLatency = Math.Max(0, s.LatencyMs - s.JitterMs);
            _net.SimulationMaxLatency = s.LatencyMs + s.JitterMs;

            _net.PollEvents();
        }

        public void Stop() => _net.Stop();

        // ── LiteNetLib INetEventListener ──

        public void OnConnectionRequest(LNL.ConnectionRequest request) => request.AcceptIfKey(_key);

        public void OnPeerConnected(LNL.NetPeer peer)
        {
            // LiteNetLib's NetPeer derives from IPEndPoint, so it IS the remote endpoint (used by per-IP policy).
            var netPeer = new NetPeer((uint)peer.Id,
                (data, offset, len, method) => peer.Send(new ReadOnlySpan<byte>(data, offset, len), MapOut(method)),
                () => _net.DisconnectPeer(peer),
                peer);

            peer.Tag = netPeer;
            if (!_isServer) _clientPeer = netPeer;
            _app.OnPeerConnected(netPeer);
        }

        public void OnPeerDisconnected(LNL.NetPeer peer, LNL.DisconnectInfo disconnectInfo)
        {
            if (peer.Tag is NetPeer netPeer)
                _app.OnPeerDisconnected(netPeer, DisconnectReason.PeerDisconnected);

            peer.Tag = null;
            if (!_isServer) _clientPeer = null;
        }

        public void OnNetworkReceive(LNL.NetPeer peer, LNL.NetPacketReader reader, byte channel, LNL.DeliveryMethod deliveryMethod)
        {
            if (peer.Tag is not NetPeer netPeer) { reader.Recycle(); return; }

            ReadOnlySpan<byte> span = reader.GetRemainingBytesSpan();
            DeliveryMethod method = MapIn(deliveryMethod);

            using (var buffer = new BitBuffer())
            {
                buffer.FromSpan(span);
                _app.OnNetworkReceive(netPeer, buffer, method);
            }

            _dispatchBuffer.Clear();
            _dispatchBuffer.FromSpan(span);
            _packets.Dispatch(netPeer, ref _dispatchBuffer);

            reader.Recycle();
        }

        public void OnNetworkError(IPEndPoint endPoint, SocketError socketError) => _app.OnNetworkError(socketError);

        public void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, LNL.NetPacketReader reader, LNL.UnconnectedMessageType messageType) { }

        public void OnNetworkLatencyUpdate(LNL.NetPeer peer, int latency) { }

        // Implemented explicitly (no reliance on default interface methods — safer for Unity IL2CPP).
        public void OnMessageDelivered(LNL.NetPeer peer, object userData) { }
        public void OnNtpResponse(LNL.Utils.NtpPacket packet) { }
        public void OnPeerAddressChanged(LNL.NetPeer peer, IPEndPoint previousAddress) { }

        // ── DeliveryMethod mapping (ours ↔ LiteNetLib) ──

        private static LNL.DeliveryMethod MapOut(DeliveryMethod m) => m switch
        {
            DeliveryMethod.Unreliable      => LNL.DeliveryMethod.Unreliable,
            DeliveryMethod.Reliable        => LNL.DeliveryMethod.ReliableUnordered,
            DeliveryMethod.ReliableOrdered => LNL.DeliveryMethod.ReliableOrdered,
            DeliveryMethod.Sequenced       => LNL.DeliveryMethod.Sequenced,
            _                              => LNL.DeliveryMethod.ReliableOrdered,
        };

        private static DeliveryMethod MapIn(LNL.DeliveryMethod m) => m switch
        {
            LNL.DeliveryMethod.Unreliable       => DeliveryMethod.Unreliable,
            LNL.DeliveryMethod.ReliableUnordered => DeliveryMethod.Reliable,
            LNL.DeliveryMethod.ReliableOrdered  => DeliveryMethod.ReliableOrdered,
            LNL.DeliveryMethod.Sequenced        => DeliveryMethod.Sequenced,
            LNL.DeliveryMethod.ReliableSequenced => DeliveryMethod.Reliable,
            _                                   => DeliveryMethod.ReliableOrdered,
        };
    }
}
