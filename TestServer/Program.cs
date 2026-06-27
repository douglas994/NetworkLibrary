using System;
using System.Threading;
using NetworkLibrary;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace TestServer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=====================================");
            Console.WriteLine("       TEST SERVER (TCP & UDP)       ");
            Console.WriteLine("=====================================");

            var listener = new EventBasedNetListener();

            listener.PeerConnectedEvent += (peer) =>
            {
                // We don't inherently know if it's UDP or TCP from the peer ID unless we track it
                Console.WriteLine($"[SERVER] Peer connected! ID: {peer.Id}");

                using var writer = new BitBuffer();
                writer.AddString($"Welcome to the server! Your ID is {peer.Id}");
                peer.Send(writer, DeliveryMethod.ReliableOrdered);
            };

            listener.PeerDisconnectedEvent += (peer, reason) =>
            {
                Console.WriteLine($"[SERVER] Peer disconnected! ID: {peer.Id}. Reason: {reason}");
            };

            listener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                string msg = reader.ReadString();
                Console.WriteLine($"[SERVER] Received from {peer.Id}: {msg} (Delivery: {method})");

                // Echo back
                using var writer = new BitBuffer();
                writer.AddString($"Server Echo: {msg}");
                peer.Send(writer, method);
            };

            var udpServer = new NetNode(listener, TransportType.Udp);
            var tcpServer = new NetNode(listener, TransportType.Tcp);

            udpServer.Start(7777);
            tcpServer.Start(7778);

            Console.WriteLine("UDP Server listening on port 7777...");
            Console.WriteLine("TCP Server listening on port 7778...");
            Console.WriteLine("Press Ctrl+C to stop.");

            // Game Loop Simulation
            while (true)
            {
                udpServer.PollEvents();
                tcpServer.PollEvents();
                Thread.Sleep(15); // ~60 FPS
            }
        }
    }
}
