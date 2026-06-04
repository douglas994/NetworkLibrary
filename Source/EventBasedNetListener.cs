/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Core API - EventBasedNetListener
 *
 *  A utility class that implements INetEventListener and exposes C# events/Actions.
 *  Perfect for users who prefer lambdas over implementing interfaces.
 */

using System;
using System.Net.Sockets;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary
{
    /// <summary>
    /// Utility listener for mapping network events to C# delegates/lambdas.
    /// </summary>
    public class EventBasedNetListener : INetEventListener
    {
        public Action<NetPeer>? PeerConnectedEvent;
        public Action<NetPeer, DisconnectReason>? PeerDisconnectedEvent;
        public Action<NetPeer, BitBuffer, DeliveryMethod>? NetworkReceiveEvent;
        public Action<SocketError>? NetworkErrorEvent;

        public void OnPeerConnected(NetPeer peer)
        {
            PeerConnectedEvent?.Invoke(peer);
        }

        public void OnPeerDisconnected(NetPeer peer, DisconnectReason reason)
        {
            PeerDisconnectedEvent?.Invoke(peer, reason);
        }

        public void OnNetworkReceive(NetPeer peer, BitBuffer reader, DeliveryMethod deliveryMethod)
        {
            NetworkReceiveEvent?.Invoke(peer, reader, deliveryMethod);
        }

        public void OnNetworkError(SocketError socketError)
        {
            NetworkErrorEvent?.Invoke(socketError);
        }
    }
}
