using System.Net;
using Xunit;
using NetworkLibrary.Transport;

namespace NetworkLibrary.Tests
{
    public class PeerAddressTests
    {
        [Theory]
        [InlineData("192.168.1.50", 7777)]
        [InlineData("127.0.0.1", 1)]
        [InlineData("255.255.255.255", 65535)]
        public void IPv4_RoundTrip(string ip, int port)
        {
            var ep = new IPEndPoint(IPAddress.Parse(ip), port);
            var key = new PeerAddress(ep);
            var back = key.ToIPEndPoint();

            Assert.False(key.IsIPv6);
            Assert.Equal(ep, back);
        }

        [Theory]
        [InlineData("::1", 7777)]
        [InlineData("2001:db8::1", 9000)]
        [InlineData("fe80::1ff:fe23:4567:890a", 1234)]
        public void IPv6_RoundTrip(string ip, int port)
        {
            var ep = new IPEndPoint(IPAddress.Parse(ip), port);
            var key = new PeerAddress(ep);
            var back = key.ToIPEndPoint();

            Assert.True(key.IsIPv6);
            Assert.Equal(ep.Address, back.Address);
            Assert.Equal(ep.Port, back.Port);
        }

        [Fact]
        public void SameEndpoint_ProducesEqualKeyAndHash()
        {
            var a = new PeerAddress(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5000));
            var b = new PeerAddress(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5000));

            Assert.True(a == b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void DifferentPortOrAddress_ProducesDifferentKey()
        {
            var baseKey = new PeerAddress(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5000));
            var diffPort = new PeerAddress(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5001));
            var diffAddr = new PeerAddress(new IPEndPoint(IPAddress.Parse("10.0.0.2"), 5000));

            Assert.NotEqual(baseKey, diffPort);
            Assert.NotEqual(baseKey, diffAddr);
        }

        [Fact]
        public void IPv4AndIPv6_AreNeverEqual()
        {
            var v4 = new PeerAddress(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 7777));
            var v6 = new PeerAddress(new IPEndPoint(IPAddress.Parse("::1"), 7777));

            Assert.NotEqual(v4, v6);
        }
    }
}
