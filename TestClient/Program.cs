using System;
using System.Threading;
using System.Threading.Tasks;
using NetworkLibrary;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace TestClient
{
    class Program
    {
        static NetPeer? udpPeer;
        static NetPeer? tcpPeer;

        static void Main(string[] args)
        {
            Console.WriteLine("=====================================");
            Console.WriteLine("       TEST CLIENT (TCP & UDP)       ");
            Console.WriteLine("=====================================");

            var udpListener = new EventBasedNetListener();
            var tcpListener = new EventBasedNetListener();

            // UDP Events
            udpListener.PeerConnectedEvent += (peer) => {
                Console.WriteLine("[CLIENT] UDP Connected to server!");
                udpPeer = peer;
            };
            udpListener.NetworkReceiveEvent += (peer, reader, method) => {
                Console.WriteLine($"[UDP MESSAGE] {reader.ReadString()}");
            };

            // TCP Events
            tcpListener.PeerConnectedEvent += (peer) => {
                Console.WriteLine("[CLIENT] TCP Connected to server!");
                tcpPeer = peer;
            };
            tcpListener.NetworkReceiveEvent += (peer, reader, method) => {
                Console.WriteLine($"[TCP MESSAGE] {reader.ReadString()}");
            };

            var udpClient = new NetNode(udpListener, TransportType.Udp);
            var tcpClient = new NetNode(tcpListener, TransportType.Tcp);

            udpClient.Connect("127.0.0.1", 7777);
            tcpClient.Connect("127.0.0.1", 7778);

            Console.WriteLine("Connecting to servers...");
            Console.WriteLine("Commands to send messages:");
            Console.WriteLine("  Type 'udp <message>' to send via UDP");
            Console.WriteLine("  Type 'tcp <message>' to send via TCP");
            Console.WriteLine("-------------------------------------");

            // Input loop on a separate task so it doesn't block the PollEvents
            Task.Run(() =>
            {
                while (true)
                {
                    string? input = Console.ReadLine();
                    if (string.IsNullOrEmpty(input)) continue;

                    if (input.StartsWith("udp ", StringComparison.OrdinalIgnoreCase))
                    {
                        if (udpPeer == null) Console.WriteLine("UDP not connected yet!");
                        else
                        {
                            using var writer = new BitBuffer();
                            writer.AddString(input.Substring(4));
                            udpPeer.Send(writer, DeliveryMethod.ReliableOrdered);
                            Console.WriteLine("-> Sent via UDP!");
                        }
                    }
                    else if (input.StartsWith("tcp ", StringComparison.OrdinalIgnoreCase))
                    {
                        if (tcpPeer == null) Console.WriteLine("TCP not connected yet!");
                        else
                        {
                            using var writer = new BitBuffer();
                            writer.AddString(input.Substring(4));
                            tcpPeer.Send(writer, DeliveryMethod.ReliableOrdered);
                            Console.WriteLine("-> Sent via TCP!");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Invalid command. Prefix with 'udp ' or 'tcp '.");
                    }
                }
            });

            // Game Loop Simulation
            while (true)
            {
                udpClient.PollEvents();
                tcpClient.PollEvents();
                Thread.Sleep(15); // ~60 FPS
            }
        }
    }
}
