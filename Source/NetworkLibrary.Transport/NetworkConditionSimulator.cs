using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace NetworkLibrary.Transport
{
    public class NetworkConditionSimulator
    {
        public bool Enabled { get; set; }
        public int LatencyMs { get; set; } = 0;
        public int JitterMs { get; set; } = 0;
        public float PacketLossPercent { get; set; } = 0f;

        private class DelayedPacket
        {
            public byte[] Data = new byte[PacketHeader.SafeMTU];
            public int Length;
            public EndPoint? EndPoint;
            public long SendTime;
        }

        private readonly List<DelayedPacket> _delayedPackets = new List<DelayedPacket>();
        private readonly Queue<DelayedPacket> _packetPool = new Queue<DelayedPacket>();
        private readonly Random _random = new Random();

        public void Send(Socket socket, byte[] data, int offset, int length, EndPoint endPoint)
        {
            if (!Enabled || (LatencyMs == 0 && PacketLossPercent == 0f))
            {
                socket.SendTo(data, offset, length, SocketFlags.None, endPoint);
                return;
            }

            // Packet Loss
            if (PacketLossPercent > 0f)
            {
                if (_random.NextDouble() * 100.0 < PacketLossPercent)
                {
                    return; // Dropped
                }
            }

            // Latency / Jitter
            if (LatencyMs > 0)
            {
                int delay = LatencyMs;
                if (JitterMs > 0)
                {
                    delay += _random.Next(-JitterMs, JitterMs + 1);
                }
                
                if (delay < 0) delay = 0;

                long now = Stopwatch.GetTimestamp();
                long delayTicks = delay * Stopwatch.Frequency / 1000;

                DelayedPacket packet;
                if (_packetPool.Count > 0)
                    packet = _packetPool.Dequeue();
                else
                    packet = new DelayedPacket();

                Buffer.BlockCopy(data, offset, packet.Data, 0, length);
                packet.Length = length;
                packet.EndPoint = endPoint;
                packet.SendTime = now + delayTicks;

                _delayedPackets.Add(packet);
            }
            else
            {
                socket.SendTo(data, offset, length, SocketFlags.None, endPoint);
            }
        }

        public void Tick(Socket socket)
        {
            if (!Enabled || _delayedPackets.Count == 0) return;

            long now = Stopwatch.GetTimestamp();

            for (int i = _delayedPackets.Count - 1; i >= 0; i--)
            {
                var packet = _delayedPackets[i];
                if (now >= packet.SendTime)
                {
                    try
                    {
                        socket.SendTo(packet.Data, 0, packet.Length, SocketFlags.None, packet.EndPoint!);
                    }
                    catch (SocketException) { }

                    _delayedPackets.RemoveAt(i);
                    _packetPool.Enqueue(packet);
                }
            }
        }
    }
}
