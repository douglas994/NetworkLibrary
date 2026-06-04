/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - Packet Header
 *
 *  Defines the packet header format and channel types.
 *  Every UDP packet starts with this header for routing and reliability.
 */

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Delivery channel types for UDP packets.
    /// </summary>
    public enum DeliveryMethod : byte
    {
        /// <summary>Fire and forget. No guarantees. Lowest overhead. (movement, effects)</summary>
        Unreliable = 0,

        /// <summary>Guaranteed delivery, no ordering guarantee. (damage, loot drops)</summary>
        Reliable = 1,

        /// <summary>Guaranteed delivery and ordering. Highest overhead. (inventory, quests, chat)</summary>
        ReliableOrdered = 2,

        /// <summary>No delivery guarantee, but stale packets are dropped. (position updates)</summary>
        Sequenced = 3,

        /// <summary>A fragment of a larger reliable packet.</summary>
        ReliableFragment = 4,

        /// <summary>A fragment of a larger reliable ordered packet.</summary>
        ReliableOrderedFragment = 5,

        /// <summary>Internal: connection management (connect, disconnect, ping).</summary>
        Internal = 255
    }

    /// <summary>
    /// Internal packet types for connection management.
    /// </summary>
    internal enum InternalPacketType : byte
    {
        ConnectRequest = 0,
        ConnectAccept = 1,
        ConnectReject = 2,
        Disconnect = 3,
        Ping = 4,
        Pong = 5,
        Fragment = 6
    }

    /// <summary>
    /// UDP packet header layout (7 bytes total):
    /// [Protocol ID: 1 byte] [Channel: 1 byte] [Sequence: 2 bytes] [Ack: 2 bytes] [AckBits: 4 bytes] [DataLength: 2 bytes]
    /// Total: 12 bytes overhead per packet.
    /// </summary>
    public static class PacketHeader
    {
        /// <summary>Protocol identifier to filter out garbage packets.</summary>
        public const byte ProtocolId = 0x4E; // 'N' for NetworkLibrary

        /// <summary>Header size in bytes.</summary>
        public const int HeaderSize = 12;

        /// <summary>Maximum payload size per UDP packet (MTU - IP header - UDP header - our header).</summary>
        public const int MaxPayloadSize = 1200 - HeaderSize; // ~1188 bytes

        /// <summary>Maximum Transfer Unit for safe UDP.</summary>
        public const int SafeMTU = 1200;

        /// <summary>
        /// Writes a packet header into the destination buffer.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write(byte[] buffer, DeliveryMethod delivery, ushort sequence, ushort ack, uint ackBits, ushort dataLength)
        {
            buffer[0] = ProtocolId;
            buffer[1] = (byte)delivery;
            buffer[2] = (byte)(sequence & 0xFF);
            buffer[3] = (byte)(sequence >> 8);
            buffer[4] = (byte)(ack & 0xFF);
            buffer[5] = (byte)(ack >> 8);
            buffer[6] = (byte)(ackBits & 0xFF);
            buffer[7] = (byte)((ackBits >> 8) & 0xFF);
            buffer[8] = (byte)((ackBits >> 16) & 0xFF);
            buffer[9] = (byte)((ackBits >> 24) & 0xFF);
            buffer[10] = (byte)(dataLength & 0xFF);
            buffer[11] = (byte)(dataLength >> 8);
        }

        /// <summary>
        /// Reads a packet header from the source buffer.
        /// Returns false if the protocol ID doesn't match.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Read(byte[] buffer, int length, out DeliveryMethod delivery, out ushort sequence, out ushort ack, out uint ackBits, out ushort dataLength)
        {
            delivery = DeliveryMethod.Unreliable;
            sequence = 0;
            ack = 0;
            ackBits = 0;
            dataLength = 0;

            if (length < HeaderSize)
                return false;

            if (buffer[0] != ProtocolId)
                return false;

            delivery = (DeliveryMethod)buffer[1];
            sequence = (ushort)(buffer[2] | (buffer[3] << 8));
            ack = (ushort)(buffer[4] | (buffer[5] << 8));
            ackBits = (uint)(buffer[6] | (buffer[7] << 8) | (buffer[8] << 16) | (buffer[9] << 24));
            dataLength = (ushort)(buffer[10] | (buffer[11] << 8));

            return true;
        }

        /// <summary>
        /// Checks if sequence A is more recent than sequence B,
        /// handling ushort wraparound correctly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSequenceNewer(ushort a, ushort b)
        {
            return ((a > b) && (a - b <= 32768)) ||
                   ((a < b) && (b - a > 32768));
        }

        /// <summary>
        /// Calculates the distance between two sequence numbers, handling wraparound.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SequenceDistance(ushort a, ushort b)
        {
            int diff = a - b;

            if (diff > 32768)
                diff -= 65536;
            else if (diff < -32768)
                diff += 65536;

            return diff;
        }
    }
}
