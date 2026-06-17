/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - NetworkPeer
 *
 *  Represents a single network connection (one client).
 *  Manages per-connection state: reliability channels, sequencing, RTT, fragmentation.
 */

using System;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Connection state of a peer.
    /// </summary>
    public enum PeerState : byte
    {
        /// <summary>No connection.</summary>
        Disconnected = 0,

        /// <summary>Connection handshake in progress.</summary>
        Connecting = 1,

        /// <summary>Connected and active.</summary>
        Connected = 2,

        /// <summary>Disconnect requested, waiting for final ACKs.</summary>
        Disconnecting = 3
    }

    /// <summary>
    /// Represents a single remote peer (client or server).
    /// Manages per-connection reliability, sequencing, and statistics.
    /// </summary>
    public sealed class NetworkPeer
    {
        /// <summary>Unique peer identifier assigned by the server.</summary>
        public uint PeerId { get; internal set; }

        /// <summary>Remote endpoint address.</summary>
        public EndPoint RemoteEndPoint { get; internal set; }

        /// <summary>
        /// Pre-serialized destination address, cached once at accept time.
        /// Lets the send path call Socket.SendTo(span, flags, SocketAddress) without
        /// re-serializing the IPEndPoint (and allocating a SocketAddress) on every packet.
        /// </summary>
        internal System.Net.SocketAddress? _sendAddress;

        /// <summary>Zero-allocation internal address key.</summary>
        internal PeerAddress InternalAddress { get; set; }

        /// <summary>Current connection state.</summary>
        public PeerState State { get; internal set; }

        /// <summary>Round-trip time in milliseconds.</summary>
        public float RTT => _reliableChannel.RTT;

        /// <summary>Time of last received packet (Stopwatch ticks).</summary>
        public long LastReceiveTime { get; internal set; }

        /// <summary>Time of last sent packet (Stopwatch ticks).</summary>
        public long LastSendTime { get; internal set; }

        // Channels
        internal readonly ReliableChannel _reliableChannel;
        internal readonly ReliableChannel _reliableOrderedChannel;
        internal readonly FragmentChannel _fragmentChannel;

        // Set when a reliable/ordered packet is received → a dedicated Ack packet is flushed next Update (batched).
        internal bool NeedsAck;

        // Unreliable sequencing
        private ushort _unreliableLocalSequence;
        private ushort _unreliableRemoteSequence;

        // Sequenced channel (newest only)
        private ushort _sequencedLocalSequence;
        private ushort _sequencedRemoteSequence;

        // Connection management
        internal long _connectTimestamp;
        internal int _connectAttempts;
        internal long _disconnectTimestamp;

        // Ping
        internal long _lastPingTime;
        internal ushort _pingSequence;

        /// <summary>Custom user data associated with this peer.</summary>
        public object? UserData { get; set; }

        public NetworkPeer()
        {
            _reliableChannel = new ReliableChannel();
            _reliableOrderedChannel = new ReliableChannel();
            _fragmentChannel = new FragmentChannel();
            RemoteEndPoint = null!;
            Reset();
        }

        /// <summary>
        /// Gets the next sequence number for unreliable sends.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ushort NextUnreliableSequence()
        {
            return _unreliableLocalSequence++;
        }

        /// <summary>
        /// Gets the next sequence number for sequenced sends.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ushort NextSequencedSequence()
        {
            return _sequencedLocalSequence++;
        }

        /// <summary>
        /// Checks if an unreliable packet is stale (older than the most recent received).
        /// Returns true if the packet should be processed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ProcessUnreliableSequence(ushort sequence)
        {
            // Unreliable doesn't care about ordering — accept all
            if (PacketHeader.IsSequenceNewer(sequence, _unreliableRemoteSequence))
            {
                _unreliableRemoteSequence = sequence;
            }
            return true;
        }

        /// <summary>
        /// Checks if a sequenced packet is newer than the last received.
        /// Returns true if this packet should be processed (is the newest).
        /// Stale packets are silently dropped.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ProcessSequencedPacket(ushort sequence)
        {
            if (PacketHeader.IsSequenceNewer(sequence, _sequencedRemoteSequence))
            {
                _sequencedRemoteSequence = sequence;
                return true;
            }
            return false; // Stale, drop it
        }

        /// <summary>
        /// Resets all peer state for reuse (connection pool pattern).
        /// </summary>
        public void Reset()
        {
            PeerId = 0;
            State = PeerState.Disconnected;
            LastReceiveTime = 0;
            LastSendTime = 0;
            UserData = null;

            _unreliableLocalSequence = 0;
            _unreliableRemoteSequence = 0;
            _sequencedLocalSequence = 0;
            _sequencedRemoteSequence = 0;

            _connectTimestamp = 0;
            _connectAttempts = 0;
            _disconnectTimestamp = 0;
            _lastPingTime = 0;
            _pingSequence = 0;

            NeedsAck = false;
            _reliableChannel.Reset();
            _reliableOrderedChannel.Reset();
        }

        /// <summary>
        /// Gets combined statistics for this peer.
        /// </summary>
        public PeerStatistics GetStatistics()
        {
            return new PeerStatistics
            {
                RTT = RTT,
                PacketsSent = _reliableChannel.PacketsSent + _reliableOrderedChannel.PacketsSent,
                PacketsReceived = _reliableChannel.PacketsReceived + _reliableOrderedChannel.PacketsReceived,
                PacketsLost = _reliableChannel.PacketsLost + _reliableOrderedChannel.PacketsLost
            };
        }
    }

    /// <summary>
    /// Statistics for a single peer connection.
    /// </summary>
    public struct PeerStatistics
    {
        public float RTT;
        public int PacketsSent;
        public int PacketsReceived;
        public int PacketsLost;
    }
}
