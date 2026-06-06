using System;
using System.Net;
using System.Net.Sockets;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Zero-allocation identifier for a network peer (IPv4 or IPv6 + port).
    /// Used as a dictionary key to avoid IPEndPoint allocations during packet receive.
    /// Stores the address as 16 bytes (IPv4 is held in the low 4 bytes) plus port and IPv6 scope id.
    /// </summary>
    public readonly struct PeerAddress : IEquatable<PeerAddress>
    {
        // Address bytes. For IPv6: _hi = bytes 0..7, _lo = bytes 8..15.
        // For IPv4: _hi = 0, _lo = the 4 address bytes packed big-endian in the low 32 bits.
        private readonly ulong _hi;
        private readonly ulong _lo;
        private readonly uint _scopeId; // IPv6 scope id (link-local); 0 for IPv4
        private readonly ushort _port;
        private readonly bool _isIPv6;

        /// <summary>True if this address is IPv6.</summary>
        public bool IsIPv6 => _isIPv6;

        /// <summary>
        /// Builds a key directly from a raw socket address (the zero-allocation hot path).
        /// Supports both InterNetwork (IPv4) and InterNetworkV6 layouts.
        /// </summary>
        public PeerAddress(SocketAddress socketAddress)
        {
            // Common sockaddr layout: [0-1] family, [2-3] port (network byte order).
            ushort port = (ushort)((socketAddress[2] << 8) | socketAddress[3]);

            if (socketAddress.Family == AddressFamily.InterNetworkV6)
            {
                // sockaddr_in6: [4-7] flowinfo, [8-23] address (16 bytes), [24-27] scope id.
                ulong hi = 0;
                for (int i = 8; i < 16; i++)
                    hi = (hi << 8) | socketAddress[i];

                ulong lo = 0;
                for (int i = 16; i < 24; i++)
                    lo = (lo << 8) | socketAddress[i];

                _hi = hi;
                _lo = lo;
                _scopeId = (uint)(socketAddress[24] | (socketAddress[25] << 8) | (socketAddress[26] << 16) | (socketAddress[27] << 24));
                _port = port;
                _isIPv6 = true;
            }
            else
            {
                // sockaddr_in: [4-7] IPv4 address.
                ulong addr = ((ulong)socketAddress[4] << 24) | ((ulong)socketAddress[5] << 16)
                           | ((ulong)socketAddress[6] << 8) | socketAddress[7];
                _hi = 0;
                _lo = addr;
                _scopeId = 0;
                _port = port;
                _isIPv6 = false;
            }
        }

        /// <summary>
        /// Builds a key from an IPEndPoint (used for user-facing events / outgoing addresses).
        /// </summary>
        public PeerAddress(IPEndPoint endPoint)
        {
            _port = (ushort)endPoint.Port;

            if (endPoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                Span<byte> b = stackalloc byte[16];
                endPoint.Address.TryWriteBytes(b, out _);

                ulong hi = 0;
                for (int i = 0; i < 8; i++)
                    hi = (hi << 8) | b[i];

                ulong lo = 0;
                for (int i = 8; i < 16; i++)
                    lo = (lo << 8) | b[i];

                _hi = hi;
                _lo = lo;
                _scopeId = (uint)endPoint.Address.ScopeId;
                _isIPv6 = true;
            }
            else
            {
                Span<byte> b = stackalloc byte[4];
                endPoint.Address.TryWriteBytes(b, out _);
                _hi = 0;
                _lo = ((ulong)b[0] << 24) | ((ulong)b[1] << 16) | ((ulong)b[2] << 8) | b[3];
                _scopeId = 0;
                _isIPv6 = false;
            }
        }

        /// <summary>Reconstructs an IPEndPoint (allocates — use only for events, not the hot path).</summary>
        public IPEndPoint ToIPEndPoint()
        {
            if (_isIPv6)
            {
                Span<byte> b = stackalloc byte[16];
                for (int i = 0; i < 8; i++)
                    b[i] = (byte)(_hi >> (56 - i * 8));
                for (int i = 0; i < 8; i++)
                    b[8 + i] = (byte)(_lo >> (56 - i * 8));
                return new IPEndPoint(new IPAddress(b, _scopeId), _port);
            }
            else
            {
                Span<byte> b = stackalloc byte[4];
                b[0] = (byte)(_lo >> 24);
                b[1] = (byte)(_lo >> 16);
                b[2] = (byte)(_lo >> 8);
                b[3] = (byte)_lo;
                return new IPEndPoint(new IPAddress(b), _port);
            }
        }

        public bool Equals(PeerAddress other) =>
            _lo == other._lo && _hi == other._hi && _port == other._port &&
            _scopeId == other._scopeId && _isIPv6 == other._isIPv6;

        public override bool Equals(object? obj) => obj is PeerAddress other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(_hi, _lo, _port, _scopeId, _isIPv6);

        public static bool operator ==(PeerAddress left, PeerAddress right) => left.Equals(right);
        public static bool operator !=(PeerAddress left, PeerAddress right) => !left.Equals(right);
    }
}
