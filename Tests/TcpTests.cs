using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using NetworkLibrary.Transport;
using NetworkLibrary.Serialization;

namespace NetworkLibrary.Tests
{
    public class TcpTests
    {
        [Fact]
        public async Task Tcp_ConnectionAndMessage_RoundTrip()
        {
            int testPort = 12345;
            using var server = new TcpServer();
            using var client = new TcpClient();

            bool serverClientConnected = false;
            bool clientConnected = false;
            bool serverReceivedData = false;
            bool clientReceivedData = false;

            TcpPeer? connectedPeer = null;

            // ─── SERVER SETUP ───
            server.OnPeerConnected = (peer) => 
            {
                serverClientConnected = true;
                connectedPeer = peer;
            };

            server.OnDataReceived = (peer, data) => 
            {
                // Carrega os bytes recebidos em um novo BitBuffer
                using var reader = new BitBuffer();
                reader.FromArray(data);

                // Valida o que o cliente mandou
                Assert.Equal(42u, reader.ReadUInt());
                Assert.Equal("Hello Server!", reader.ReadString());

                serverReceivedData = true;

                // Servidor responde
                using var response = new BitBuffer();
                response.AddString("Hello Client!");
                server.SendTo(peer.PeerId, response);
            };

            server.Start(testPort);
            Assert.True(server.IsRunning);

            // ─── CLIENT SETUP ───
            client.OnConnected = () => 
            {
                clientConnected = true;

                // Cliente manda mensagem assim que conecta
                using var writer = new BitBuffer();
                writer.AddUInt(42u);
                writer.AddString("Hello Server!");
                client.Send(writer);
            };

            client.OnDataReceived = (data) =>
            {
                using var reader = new BitBuffer();
                reader.FromArray(data);
                
                Assert.Equal("Hello Client!", reader.ReadString());
                clientReceivedData = true;
            };

            client.Connect("127.0.0.1", testPort);

            // ─── GAME LOOP SIMULATION ───
            // Simula o loop do jogo rodando a 60 FPS por no máximo 2 segundos
            int timeoutFrames = 120;
            while (timeoutFrames > 0 && (!serverReceivedData || !clientReceivedData))
            {
                // Como configuramos, os eventos SÓ disparam quando chamamos Update()
                // Isso protege a Main Thread (Unity)
                server.Update();
                client.Update();

                await Task.Delay(16); // ~60fps
                timeoutFrames--;
            }

            // ─── ASSERTIONS ───
            Assert.True(serverClientConnected, "Servidor não registrou a conexão do cliente.");
            Assert.True(clientConnected, "Cliente não registrou OnConnected.");
            Assert.True(serverReceivedData, "Servidor não recebeu os dados do cliente.");
            Assert.True(clientReceivedData, "Cliente não recebeu os dados do servidor.");

            Assert.NotNull(connectedPeer);

            // ─── TEARDOWN ───
            client.Disconnect();
            
            timeoutFrames = 10;
            bool peerDisconnected = false;
            server.OnPeerDisconnected = (peer) => peerDisconnected = true;

            while (timeoutFrames > 0 && !peerDisconnected)
            {
                server.Update();
                client.Update();
                await Task.Delay(16);
                timeoutFrames--;
            }

            Assert.True(peerDisconnected, "Servidor não registrou o disconnect do cliente.");
        }
    }
}
