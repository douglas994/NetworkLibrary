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

        /// <summary>A dedicated ACK packet (carries both reliable channels' ACK state; no app payload).</summary>
        Ack = 6,

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
    /// UDP packet header layout (6 bytes total): [Protocol ID: 1] [Channel: 1] [Sequence: 2] [DataLength: 2].
    /// ACKs are NOT piggybacked on the data header (that bloated every snapshot/movement packet). Instead a receiver
    /// sends a dedicated <see cref="DeliveryMethod.Ack"/> packet (both channels' ACK state, see WriteAckPayload) once per
    /// tick whenever it has received reliable data — driven by RECEIVING, not by sending, so one-way reliable traffic is
    /// still acked. This keeps the hot unreliable path tiny while staying robust under loss (LiteNetLib-style).
    /// </summary>
    public static class PacketHeader
    {
        /// <summary>Protocol identifier to filter out garbage packets.</summary>
        public const byte ProtocolId = 0x4E; // 'N' for NetworkLibrary

        /// <summary>Header size in bytes.</summary>
        public const int HeaderSize = 6;

        /// <summary>Payload of a dedicated Ack packet: [ack:2][ackBits:8][ackOrdered:2][ackBitsOrdered:8] = 20 bytes.</summary>
        public const int AckPayloadSize = 20;

        /// <summary>Maximum payload size per UDP packet (MTU - IP header - UDP header - our header).</summary>
        public const int MaxPayloadSize = 1200 - HeaderSize;

        /// <summary>Maximum Transfer Unit for safe UDP.</summary>
        public const int SafeMTU = 1200;

        /// <summary>Writes the compact data header (no ACKs — those ride dedicated Ack packets).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write(Span<byte> buffer, DeliveryMethod delivery, ushort sequence, ushort dataLength)
        {
            buffer[0] = ProtocolId;
            buffer[1] = (byte)delivery;
            buffer[2] = (byte)(sequence & 0xFF);
            buffer[3] = (byte)(sequence >> 8);
            buffer[4] = (byte)(dataLength & 0xFF);
            buffer[5] = (byte)(dataLength >> 8);
        }

        /// <summary>Reads the compact data header. Returns false if the protocol ID doesn't match.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Read(byte[] buffer, int length, out DeliveryMethod delivery, out ushort sequence, out ushort dataLength)
        {
            delivery = DeliveryMethod.Unreliable;
            sequence = 0;
            dataLength = 0;

            if (length < HeaderSize)
                return false;

            if (buffer[0] != ProtocolId)
                return false;

            delivery = (DeliveryMethod)buffer[1];
            sequence = (ushort)(buffer[2] | (buffer[3] << 8));
            dataLength = (ushort)(buffer[4] | (buffer[5] << 8));

            return true;
        }

        /// <summary>Writes both channels' ACK state into an Ack packet's payload (starting at <paramref name="o"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteAckPayload(Span<byte> b, int o, ushort ack, ulong ackBits, ushort ackOrdered, ulong ackBitsOrdered)
        {
            b[o]     = (byte)(ack & 0xFF);
            b[o + 1] = (byte)(ack >> 8);
            WriteU64(b, o + 2, ackBits);
            b[o + 10] = (byte)(ackOrdered & 0xFF);
            b[o + 11] = (byte)(ackOrdered >> 8);
            WriteU64(b, o + 12, ackBitsOrdered);
        }

        /// <summary>Reads both channels' ACK state from an Ack packet's payload.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReadAckPayload(byte[] b, int o, out ushort ack, out ulong ackBits, out ushort ackOrdered, out ulong ackBitsOrdered)
        {
            ack = (ushort)(b[o] | (b[o + 1] << 8));
            ackBits = ReadU64(b, o + 2);
            ackOrdered = (ushort)(b[o + 10] | (b[o + 11] << 8));
            ackBitsOrdered = ReadU64(b, o + 12);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteU64(Span<byte> b, int o, ulong v)
        {
            b[o]     = (byte)(v & 0xFF);
            b[o + 1] = (byte)((v >> 8) & 0xFF);
            b[o + 2] = (byte)((v >> 16) & 0xFF);
            b[o + 3] = (byte)((v >> 24) & 0xFF);
            b[o + 4] = (byte)((v >> 32) & 0xFF);
            b[o + 5] = (byte)((v >> 40) & 0xFF);
            b[o + 6] = (byte)((v >> 48) & 0xFF);
            b[o + 7] = (byte)((v >> 56) & 0xFF);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong ReadU64(byte[] b, int o)
        {
            return (ulong)b[o]
                 | ((ulong)b[o + 1] << 8)
                 | ((ulong)b[o + 2] << 16)
                 | ((ulong)b[o + 3] << 24)
                 | ((ulong)b[o + 4] << 32)
                 | ((ulong)b[o + 5] << 40)
                 | ((ulong)b[o + 6] << 48)
                 | ((ulong)b[o + 7] << 56);
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
