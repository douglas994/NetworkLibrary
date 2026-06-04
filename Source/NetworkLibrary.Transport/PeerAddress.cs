using System;
using System.Net;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Zero-allocation identifier for a network peer based on IPv4 address and port.
    /// Used as a dictionary key to avoid IPEndPoint allocations during packet receive.
    /// </summary>
    public readonly struct PeerAddress : IEquatable<PeerAddress>
    {
        // Holds the 4-byte IP Address and 2-byte Port. Fits perfectly in 8 bytes.
        public readonly long Value;

        public PeerAddress(SocketAddress socketAddress)
        {
            // SocketAddress buffer structure for InterNetwork (IPv4):
            // byte 0-1: AddressFamily (2)
            // byte 2-3: Port (network byte order)
            // byte 4-7: IPv4 Address (4 bytes)
            // Total 8 bytes.
            long val = 0;
            for (int i = 2; i < 8; i++)
            {
                val = (val << 8) | socketAddress[i];
            }
            Value = val;
        }

        public PeerAddress(IPEndPoint endPoint)
        {
            byte[] ipBytes = endPoint.Address.GetAddressBytes();
            long val = 0;
            
            // Port (network order approximation)
            val = (val << 8) | (byte)(endPoint.Port >> 8);
            val = (val << 8) | (byte)(endPoint.Port & 0xFF);

            // IP
            for (int i = 0; i < 4; i++)
            {
                val = (val << 8) | ipBytes[i];
            }
            Value = val;
        }

        // To generate a real IPEndPoint if needed (e.g. for user events)
        public IPEndPoint ToIPEndPoint()
        {
            long temp = Value;
            byte[] ipBytes = new byte[4];
            for (int i = 3; i >= 0; i--)
            {
                ipBytes[i] = (byte)(temp & 0xFF);
                temp >>= 8;
            }
            int port = (int)(temp & 0xFFFF);
            return new IPEndPoint(new IPAddress(ipBytes), port);
        }

        public bool Equals(PeerAddress other) => Value == other.Value;
        public override bool Equals(object? obj) => obj is PeerAddress other && Equals(other);
        public override int GetHashCode() => Value.GetHashCode();

        public static bool operator ==(PeerAddress left, PeerAddress right) => left.Equals(right);
        public static bool operator !=(PeerAddress left, PeerAddress right) => !left.Equals(right);
    }
}
