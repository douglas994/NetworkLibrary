using System;
using System.Threading.Tasks;
using Xunit;
using NetworkLibrary;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary.Tests
{
    public class UdpTests
    {
        [Fact]
        public async Task Udp_Fragmentation_LargeDataReassembly()
        {
            int testPort = 14000;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetManager(serverListener, TransportType.Udp);
            using var client = new NetManager(clientListener, TransportType.Udp);

            bool serverReceivedData = false;
            byte[]? receivedData = null;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                // Ler array gigante
                int length = reader.ReadInt();
                receivedData = new byte[length];
                for (int i = 0; i < length; i++)
                {
                    receivedData[i] = reader.ReadByte();
                }
                serverReceivedData = true;
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                // Criar pacote de 5.000 bytes (Força fragmentação, pois MTU ~1200)
                int dataSize = 5000;
                using var writer = new BitBuffer();
                writer.AddInt(dataSize);

                for (int i = 0; i < dataSize; i++)
                {
                    // Preenche com padrão verificável
                    writer.AddByte((byte)(i % 255));
                }

                // Fragmentação requer envio confiável
                peer.Send(writer, DeliveryMethod.ReliableOrdered);
            };

            client.Connect("127.0.0.1", testPort);

            // Roda o simulador de frames
            int timeoutFrames = 200;
            while (timeoutFrames > 0 && !serverReceivedData)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16);
                timeoutFrames--;
            }

            Assert.True(serverReceivedData, "Servidor não conseguiu remontar o pacote fragmentado.");
            Assert.NotNull(receivedData);
            Assert.Equal(5000, receivedData.Length);

            // Valida integridade do gigante remontado
            for (int i = 0; i < 5000; i++)
            {
                Assert.Equal((byte)(i % 255), receivedData[i]);
            }
        }

        [Fact]
        public async Task Udp_ReliableOrdered_StressTest()
        {
            int testPort = 14001;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetManager(serverListener, TransportType.Udp);
            using var client = new NetManager(clientListener, TransportType.Udp);

            int expectedSequence = 0;
            int totalMessages = 50; // Enviar 50 mensagens em rajada (testando o ACK windowing)
            bool testPassed = false;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int receivedSeq = reader.ReadInt();
                
                // Se chegar fora de ordem, o teste falha
                Assert.Equal(expectedSequence, receivedSeq);
                
                expectedSequence++;

                if (expectedSequence == totalMessages)
                {
                    testPassed = true;
                }
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                // Dispara uma rajada de 50 mensagens instantaneamente
                for (int i = 0; i < totalMessages; i++)
                {
                    using var writer = new BitBuffer();
                    writer.AddInt(i);
                    peer.Send(writer, DeliveryMethod.ReliableOrdered);
                }
            };

            client.Connect("127.0.0.1", testPort);

            int timeoutFrames = 200;
            while (timeoutFrames > 0 && !testPassed)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16);
                timeoutFrames--;
            }

            Assert.True(testPassed, $"Falhou no stress test. Recebeu apenas {expectedSequence} de {totalMessages} mensagens ordenadas.");
        }

        [Fact]
        public async Task Udp_Handshake_RejectsWrongConnectionKey()
        {
            int testPort = 14002;
            using var server = new NetManager(new EventBasedNetListener(), TransportType.Udp) { ConnectionKey = 0xABCDEF01 };

            var clientListener = new EventBasedNetListener();
            using var client = new NetManager(clientListener, TransportType.Udp) { ConnectionKey = 0xDEADBEEF }; // wrong token

            bool clientConnected = false;
            clientListener.PeerConnectedEvent += (peer) => clientConnected = true;

            server.Start(testPort);
            client.Connect("127.0.0.1", testPort);

            // Give it plenty of frames; a rejected client should NEVER connect.
            int frames = 120;
            while (frames-- > 0 && !clientConnected)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16);
            }

            Assert.False(clientConnected, "Cliente com token errado NÃO deveria ter conectado.");
        }

        [Fact]
        public async Task Udp_Handshake_AcceptsMatchingConnectionKey()
        {
            int testPort = 14003;
            using var server = new NetManager(new EventBasedNetListener(), TransportType.Udp) { ConnectionKey = 0xABCDEF01 };

            var clientListener = new EventBasedNetListener();
            using var client = new NetManager(clientListener, TransportType.Udp) { ConnectionKey = 0xABCDEF01 }; // matching token

            bool clientConnected = false;
            clientListener.PeerConnectedEvent += (peer) => clientConnected = true;

            server.Start(testPort);
            client.Connect("127.0.0.1", testPort);

            int frames = 120;
            while (frames-- > 0 && !clientConnected)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16);
            }

            Assert.True(clientConnected, "Cliente com token correto deveria ter conectado.");
        }

        [Fact]
        public async Task Udp_ReliableOrdered_SurvivesPacketLoss()
        {
            int testPort = 14004;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetManager(serverListener, TransportType.Udp);
            using var client = new NetManager(clientListener, TransportType.Udp);

            const int totalMessages = 30;
            int expectedSequence = 0;
            bool testPassed = false;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int receivedSeq = reader.ReadInt();
                // ReliableOrdered must still arrive in order despite the lossy link.
                Assert.Equal(expectedSequence, receivedSeq);
                expectedSequence++;
                if (expectedSequence == totalMessages)
                    testPassed = true;
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    using var writer = new BitBuffer();
                    writer.AddInt(i);
                    peer.Send(writer, DeliveryMethod.ReliableOrdered);
                }
            };

            client.Connect("127.0.0.1", testPort);

            // Drop 10% of packets on BOTH directions (data AND acks) — a realistic "bad network"
            // that exercises retransmission. (Much higher bidirectional loss can exceed
            // MaxRetransmits and permanently stall ordered delivery — see ReliableChannel.)
            // Set after Connect/Start so the backends (and their simulators) exist.
            server.Simulator!.Enabled = true;
            server.Simulator.PacketLossPercent = 10f;
            client.Simulator!.Enabled = true;
            client.Simulator.PacketLossPercent = 10f;

            // Use a REAL-TIME deadline (not a frame count): retransmission is RTO/wall-clock based,
            // so polling must run long enough in real time regardless of scheduler slippage.
            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 20 && !testPassed)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.True(testPassed, $"Com 10% de perda, recebeu apenas {expectedSequence}/{totalMessages} (retransmissão falhou).");
        }
        [Fact]
        public async Task Udp_ReliableOrdered_FiltersDuplicates()
        {
            int testPort = 14005;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetManager(serverListener, TransportType.Udp);
            using var client = new NetManager(clientListener, TransportType.Udp);

            const int totalMessages = 30;
            int expectedSequence = 0;
            bool testPassed = false;
            bool duplicateDetected = false;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int receivedSeq = reader.ReadInt();
                if (receivedSeq != expectedSequence)
                {
                    duplicateDetected = true; // This should never happen if filtering works
                }
                Assert.Equal(expectedSequence, receivedSeq);
                expectedSequence++;
                if (expectedSequence == totalMessages)
                    testPassed = true;
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    using var writer = new BitBuffer();
                    writer.AddInt(i);
                    peer.Send(writer, DeliveryMethod.ReliableOrdered);
                }
            };

            client.Connect("127.0.0.1", testPort);

            // 50% packet duplication (high probability of testing the duplicate filter)
            client.Simulator!.Enabled = true;
            client.Simulator.DuplicatePercent = 50f;

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 5 && !testPassed)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.False(duplicateDetected, "Recebeu mensagens duplicadas ou fora de ordem!");
            Assert.True(testPassed, $"Recebeu apenas {expectedSequence}/{totalMessages}.");
        }
    }
}
