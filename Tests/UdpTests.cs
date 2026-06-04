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
    }
}
