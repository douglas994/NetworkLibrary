/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Core API - NetPeer
 *
 *  The centralized abstraction for a connected player/server.
 *  Uses delegates to completely decouple from UDP/TCP implementations.
 */

using System;
using NetworkLibrary.Packets;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary
{
    /// <summary>
    /// Represents a network connection (Client or Server).
    /// Agnostic to the underlying transport (UDP or TCP).
    /// </summary>
    /// <remarks>
    /// THREAD SAFETY: Sending to a single peer is NOT thread-safe — the per-peer reliable
    /// channel (sequence numbers, send window) has no internal lock by design (for performance).
    /// Send to any given peer from ONE thread at a time. The normal game pattern is safe:
    /// send from your fixed tick loop, or fan out one thread per peer (different peers in
    /// parallel is fine; the SAME peer from multiple threads concurrently is not).
    /// Sends to DIFFERENT peers from different threads are safe.
    /// </remarks>
    public sealed class NetPeer
    {
        /// <summary>Unique ID assigned to this peer (0 if this is a client endpoint).</summary>
        public uint Id { get; }

        /// <summary>Attach any custom game data here (e.g., your Player object).</summary>
        public object? UserData { get; set; }

        private readonly Action<byte[], int, int, DeliveryMethod> _sendCallback;
        private readonly Action _disconnectCallback;

        internal NetPeer(uint id, Action<byte[], int, int, DeliveryMethod> sendCallback, Action disconnectCallback)
        {
            Id = id;
            _sendCallback = sendCallback;
            _disconnectCallback = disconnectCallback;
        }

        /// <summary>
        /// Sends a message using a BitBuffer without allocating a new byte array.
        /// </summary>
        /// <param name="buffer">The data to send.</param>
        /// <param name="deliveryMethod">How to send the data (if TCP is used, it will always be ReliableOrdered).</param>
        public void Send(BitBuffer buffer, DeliveryMethod deliveryMethod)
        {
            int len = buffer.Length;
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(len);
            buffer.ToSpan(rented);
            _sendCallback(rented, 0, len, deliveryMethod);
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }

        /// <summary>
        /// Serializes and sends a struct-based packet without heap allocation.
        /// </summary>
        public void Send<T>(ref T packet, DeliveryMethod deliveryMethod) where T : struct, INetPacket
        {
            BitBuffer buffer = new BitBuffer();
            try
            {
                packet.Serialize(ref buffer);
                Send(buffer, deliveryMethod);
            }
            finally
            {
                buffer.Dispose();
            }
        }

        /// <summary>
        /// Serializes and sends a struct-based packet with an explicit ID.
        /// </summary>
        public void Send<T>(byte packetId, ref T packet, DeliveryMethod deliveryMethod) where T : struct, INetPacket
        {
            BitBuffer buffer = new BitBuffer();
            try
            {
                buffer.AddByte(packetId);
                packet.Serialize(ref buffer);
                Send(buffer, deliveryMethod);
            }
            finally
            {
                buffer.Dispose();
            }
        }

        /// <summary>
        /// Sends raw bytes to the peer.
        /// </summary>
        public void Send(byte[] data, int offset, int length, DeliveryMethod deliveryMethod)
        {
            _sendCallback(data, offset, length, deliveryMethod);
        }

        /// <summary>
        /// Disconnects this peer.
        /// </summary>
        public void Disconnect()
        {
            _disconnectCallback();
        }
    }
}
