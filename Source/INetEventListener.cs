/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Core API - INetEventListener
 *
 *  The centralized interface for all network events, modeled after LiteNetLib.
 */

using System.Net.Sockets;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary
{
    /// <summary>
    /// Reason for a peer disconnecting.
    /// </summary>
    public enum DisconnectReason
    {
        Timeout,
        PeerDisconnected,
        ConnectionFailed,
        Error
    }

    /// <summary>
    /// Transport technology used by the NetNode.
    /// </summary>
    public enum TransportType
    {
        /// <summary>Uses custom highly-optimized UDP with Reliable channels via Sliding Windows.</summary>
        Udp,
        /// <summary>Uses highly-optimized TCP via SocketAsyncEventArgs.</summary>
        Tcp
    }

    /// <summary>
    /// Centralized interface for handling network events.
    /// Implement this in your game manager or use EventBasedNetListener.
    /// </summary>
    public interface INetEventListener
    {
        /// <summary>
        /// Called when a peer successfully connects.
        /// </summary>
        void OnPeerConnected(NetPeer peer);

        /// <summary>
        /// Called when a peer disconnects or is dropped.
        /// </summary>
        void OnPeerDisconnected(NetPeer peer, DisconnectReason reason);

        /// <summary>
        /// Called when data is received from a peer. 
        /// The BitBuffer is pre-loaded with the data and ready to read.
        /// </summary>
        void OnNetworkReceive(NetPeer peer, BitBuffer reader, DeliveryMethod deliveryMethod);
        
        /// <summary>
        /// Called when a low-level socket error occurs.
        /// </summary>
        void OnNetworkError(SocketError socketError);
    }
}
