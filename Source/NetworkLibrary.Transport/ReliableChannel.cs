/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - ReliableChannel
 *
 *  Implements reliability on top of UDP using:
 *  - Sequence numbers (ushort, wraps at 65535)
 *  - ACK bitmask (uint, confirms 32 packets at once)
 *  - Retransmission with adaptive RTT timer
 *  - Congestion avoidance
 *
 *  Based on Glenn Fiedler's "Reliable UDP" approach.
 */

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Pending packet awaiting acknowledgment.
    /// </summary>
    public struct PendingPacket
    {
        public byte[] Data;
        public int DataLength;
        public ushort Sequence;
        public long SentTimestamp; // Stopwatch ticks
        public int RetransmitCount;
        public bool Acknowledged;
        public bool Active;
    }

    /// <summary>
    /// Provides reliable delivery over UDP with ACK-based retransmission.
    /// Handles both Reliable and ReliableOrdered delivery methods.
    /// </summary>
    public sealed class ReliableChannel
    {
        /// <summary>Maximum number of in-flight packets before stalling.</summary>
        /// <remarks>Must be a power of two (indexed via WindowMask). Bumped 64→256: AoI spawn/despawn + economy/loot
        /// bursts can put well over 64 reliable packets in flight before ACKs return, and a full window silently drops
        /// reliable sends. 256 slots × 2 channels per peer is trivial RAM and gives ~4× burst headroom.</remarks>
        private const int WindowSize = 256;
        private const int WindowMask = WindowSize - 1;
        /// <summary>After this many timed-out retransmits we WARN but keep retrying — we never deactivate a pending packet,
        /// because deactivating it permanently stalls the ordered window (oldest-unacked can't advance past a dead slot).
        /// A genuinely dead connection is caught by the heartbeat/timeout instead, which resets the whole channel.</summary>
        private const int MaxRetransmits = 10;
        private static readonly long DefaultRTOTicks = Stopwatch.Frequency; // 1 second default RTO

        private readonly PendingPacket[] _sendBuffer;
        private readonly PendingPacket[] _receiveBuffer;

        private ushort _localSequence;
        private ushort _remoteSequence;
        private ushort _oldestUnackedSequence;

        // Ordered delivery tracking
        private ushort _nextOrderedSequence;

        // RTT estimation (Jacobson/Karels algorithm)
        private long _smoothedRTT;
        private long _rttVariation;
        private long _retransmissionTimeout;

        // Stats
        private int _packetsSent;
        private int _packetsReceived;
        private int _packetsLost;
        private int _packetsRetransmitted;

        /// <summary>Estimated round-trip time in milliseconds.</summary>
        public float RTT => (float)_smoothedRTT / Stopwatch.Frequency * 1000f;

        /// <summary>Total packets sent on this channel.</summary>
        public int PacketsSent => _packetsSent;

        /// <summary>Total packets received on this channel.</summary>
        public int PacketsReceived => _packetsReceived;

        /// <summary>Total packets detected as lost.</summary>
        public int PacketsLost => _packetsLost;

        /// <summary>
        /// Cheap O(1) check: true if there are packets sent but not yet acknowledged.
        /// Used to skip the full retransmit scan for idle channels.
        /// </summary>
        public bool HasUnackedPackets
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _oldestUnackedSequence != _localSequence;
        }

        public ReliableChannel()
        {
            _sendBuffer = new PendingPacket[WindowSize];
            _receiveBuffer = new PendingPacket[WindowSize];
            _localSequence = 0;
            _remoteSequence = 0;
            _oldestUnackedSequence = 0;
            _nextOrderedSequence = 0;
            _smoothedRTT = DefaultRTOTicks / 4;
            _rttVariation = 0;
            _retransmissionTimeout = DefaultRTOTicks;
        }

        /// <summary>
        /// Queues a packet for reliable delivery. Returns the assigned sequence number.
        /// </summary>
        /// <param name="data">Packet data (will be copied).</param>
        /// <param name="offset">Offset in data array.</param>
        /// <param name="length">Length of data to send.</param>
        /// <returns>Sequence number, or -1 if the send window is full.</returns>
        public int QueueSend(byte[] data, int offset, int length)
        {
            // Check if send window is full
            int inFlight = PacketHeader.SequenceDistance(_localSequence, _oldestUnackedSequence);
            if (inFlight >= WindowSize)
                return -1;

            ushort seq = _localSequence++;
            int index = seq & WindowMask;

            ref PendingPacket packet = ref _sendBuffer[index];

            if (packet.Data == null || packet.Data.Length < length)
            {
                if (packet.Data != null) ArrayPool<byte>.Shared.Return(packet.Data);
                packet.Data = ArrayPool<byte>.Shared.Rent(length);
            }

            Buffer.BlockCopy(data, offset, packet.Data, 0, length);
            packet.DataLength = length;
            packet.Sequence = seq;
            packet.SentTimestamp = Stopwatch.GetTimestamp();
            packet.RetransmitCount = 0;
            packet.Acknowledged = false;
            packet.Active = true;

            _packetsSent++;

            return seq;
        }

        /// <summary>
        /// Processes an incoming ACK, marking packets as acknowledged and updating RTT.
        /// </summary>
        /// <param name="ack">Most recent sequence number acknowledged by remote.</param>
        /// <param name="ackBits">Bitmask of 32 previous packets acknowledged (bit 0 = ack-1, bit 1 = ack-2, etc.).</param>
        public void ProcessAck(ushort ack, uint ackBits)
        {
            // Acknowledge the main ack sequence
            AcknowledgePacket(ack);

            // Acknowledge packets in the bitmask
            for (int i = 0; i < 32; i++)
            {
                if ((ackBits & (1u << i)) != 0)
                {
                    ushort seq = (ushort)(ack - 1 - i);
                    AcknowledgePacket(seq);
                }
            }

            // Advance the oldest unacked sequence
            while (PacketHeader.IsSequenceNewer(_localSequence, _oldestUnackedSequence))
            {
                int index = _oldestUnackedSequence & WindowMask;

                if (_sendBuffer[index].Active && _sendBuffer[index].Acknowledged)
                {
                    if (_sendBuffer[index].Data != null)
                    {
                        ArrayPool<byte>.Shared.Return(_sendBuffer[index].Data);
                        _sendBuffer[index].Data = null!;
                    }
                    _sendBuffer[index].Active = false;
                    _oldestUnackedSequence++;
                }
                else
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Gets packets that need retransmission (timed out without ACK).
        /// </summary>
        /// <param name="packets">Output array for packets needing retransmission.</param>
        /// <param name="maxPackets">Maximum number of packets to return.</param>
        /// <returns>Number of packets needing retransmission.</returns>
        public int GetPacketsToRetransmit(PendingPacket[] packets, int maxPackets)
        {
            long now = Stopwatch.GetTimestamp();
            int count = 0;

            for (int i = 0; i < WindowSize && count < maxPackets; i++)
            {
                ushort seq = (ushort)(_oldestUnackedSequence + i);

                if (!PacketHeader.IsSequenceNewer(_localSequence, seq) && seq != _localSequence)
                    break;

                int index = seq & WindowMask;
                ref PendingPacket packet = ref _sendBuffer[index];

                if (packet.Active && !packet.Acknowledged && packet.Sequence == seq)
                {
                    long elapsed = now - packet.SentTimestamp;

                    if (elapsed >= _retransmissionTimeout)
                    {
                        // Keep retransmitting indefinitely. We must NOT deactivate the packet after MaxRetransmits:
                        // doing so leaves a permanently-unacked hole that the oldest-unacked pointer can never advance
                        // past → the send window fills and every future reliable send is silently dropped (a permanent
                        // freeze of inventory/economy/spawn while unreliable snapshots keep flowing). A truly dead peer
                        // is reaped by the heartbeat timeout (which Resets the channel); transient loss/bursts recover.
                        if (packet.RetransmitCount == MaxRetransmits) _packetsLost++; // count once when it crosses the threshold

                        packet.SentTimestamp = now;
                        packet.RetransmitCount++;
                        _packetsRetransmitted++;

                        packets[count++] = packet;
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Processes a received reliable packet. Returns true if this is a new packet
        /// (not a duplicate). For ordered delivery, use TryGetNextOrdered instead.
        /// </summary>
        public bool ProcessReceive(ushort sequence, byte[] data, int offset, int length, bool storeData = false)
        {
            // For ordered channels, any sequence older than the next expected one was already delivered.
            if (storeData && sequence != _nextOrderedSequence && PacketHeader.IsSequenceNewer(_nextOrderedSequence, sequence))
            {
                return false;
            }

            // For all channels, drop packets that fall way outside the current receive window
            if (sequence != _remoteSequence && PacketHeader.IsSequenceNewer(_remoteSequence, sequence))
            {
                int distance = PacketHeader.SequenceDistance(sequence, _remoteSequence);
                if (distance >= WindowSize)
                    return false; // Too old, out of window
            }

            int index = sequence & WindowMask;
            ref PendingPacket slot = ref _receiveBuffer[index];

            // If the slot is active and holds a newer or equal sequence, this packet is a duplicate or too old to overwrite
            if (slot.Active)
            {
                if (slot.Sequence == sequence) 
                    return false;
                
                if (PacketHeader.IsSequenceNewer(slot.Sequence, sequence))
                    return false;
            }

            // Update remote sequence
            if (PacketHeader.IsSequenceNewer(sequence, _remoteSequence))
            {
                _remoteSequence = sequence;
            }

            // Store the packet data only if requested
            if (storeData)
            {
                if (slot.Data == null || slot.Data.Length < length)
                {
                    if (slot.Data != null) ArrayPool<byte>.Shared.Return(slot.Data);
                    slot.Data = ArrayPool<byte>.Shared.Rent(length);
                }

                Buffer.BlockCopy(data, offset, slot.Data, 0, length);
            }
            
            slot.DataLength = length;
            slot.Sequence = sequence;
            slot.Active = true;

            _packetsReceived++;

            return true;
        }

        /// <summary>
        /// For ReliableOrdered: tries to get the next packet in sequence order.
        /// Returns true and advances the ordered sequence if the next expected packet is available.
        /// </summary>
        public bool TryGetNextOrdered(out byte[] data, out int dataLength)
        {
            int index = _nextOrderedSequence & WindowMask;
            ref PendingPacket slot = ref _receiveBuffer[index];

            if (slot.Active && slot.Sequence == _nextOrderedSequence)
            {
                data = slot.Data; // Caller must return to ArrayPool
                dataLength = slot.DataLength;
                slot.Data = null!;
                slot.Active = false;
                _nextOrderedSequence++;
                return true;
            }

            data = null!;
            dataLength = 0;
            return false;
        }

        /// <summary>
        /// Generates the ACK and ACK bitmask for the current receive state.
        /// This should be included in every outgoing packet.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void GenerateAckData(out ushort ack, out uint ackBits)
        {
            ack = _remoteSequence;
            ackBits = 0;

            for (int i = 0; i < 32; i++)
            {
                ushort seq = (ushort)(_remoteSequence - 1 - i);
                int index = seq & WindowMask;

                if (_receiveBuffer[index].Active && _receiveBuffer[index].Sequence == seq)
                {
                    ackBits |= (1u << i);
                }
            }
        }

        /// <summary>
        /// Resets the channel state.
        /// </summary>
        public void Reset()
        {
            _localSequence = 0;
            _remoteSequence = 0;
            _oldestUnackedSequence = 0;
            _nextOrderedSequence = 0;
            _smoothedRTT = DefaultRTOTicks / 4;
            _rttVariation = 0;
            _retransmissionTimeout = DefaultRTOTicks;
            _packetsSent = 0;
            _packetsReceived = 0;
            _packetsLost = 0;
            _packetsRetransmitted = 0;

            for (int i = 0; i < WindowSize; i++)
            {
                if (_sendBuffer[i].Data != null)
                {
                    ArrayPool<byte>.Shared.Return(_sendBuffer[i].Data);
                    _sendBuffer[i].Data = null!;
                }
                _sendBuffer[i].Active = false;

                if (_receiveBuffer[i].Data != null)
                {
                    ArrayPool<byte>.Shared.Return(_receiveBuffer[i].Data);
                    _receiveBuffer[i].Data = null!;
                }
                _receiveBuffer[i].Active = false;
            }
        }

        // ── Private helpers ──

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AcknowledgePacket(ushort sequence)
        {
            int index = sequence & WindowMask;
            ref PendingPacket packet = ref _sendBuffer[index];

            if (packet.Active && !packet.Acknowledged && packet.Sequence == sequence)
            {
                packet.Acknowledged = true;

                // Update RTT estimate
                long rtt = Stopwatch.GetTimestamp() - packet.SentTimestamp;
                UpdateRTT(rtt);
            }
        }

        /// <summary>
        /// Updates RTT using the Jacobson/Karels algorithm (same as TCP).
        /// SRTT = (1-α)·SRTT + α·RTT     (α = 1/8)
        /// RTTVAR = (1-β)·RTTVAR + β·|SRTT - RTT|  (β = 1/4)
        /// RTO = SRTT + 4·RTTVAR
        /// </summary>
        private void UpdateRTT(long rtt)
        {
            if (_smoothedRTT == 0)
            {
                _smoothedRTT = rtt;
                _rttVariation = rtt / 2;
            }
            else
            {
                long delta = Math.Abs(_smoothedRTT - rtt);
                _rttVariation = (_rttVariation * 3 + delta) / 4;
                _smoothedRTT = (_smoothedRTT * 7 + rtt) / 8;
            }

            _retransmissionTimeout = Math.Max(
                Stopwatch.Frequency / 10, // Minimum 100ms
                _smoothedRTT + 4 * _rttVariation
            );
        }
    }
}
