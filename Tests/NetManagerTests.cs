using System;
using System.Threading.Tasks;
using Xunit;
using NetworkLibrary;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;

namespace NetworkLibrary.Tests
{
    public class NetManagerTests
    {
        [Theory]
        [InlineData(TransportType.Tcp, 13000)]
        [InlineData(TransportType.Udp, 13001)]
        public async Task NetManager_ConnectionAndMessage_RoundTrip(TransportType transportType, int port)
        {
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetManager(serverListener, transportType);
            using var client = new NetManager(clientListener, transportType);

            bool serverClientConnected = false;
            bool clientConnected = false;
            bool serverReceivedData = false;
            bool clientReceivedData = false;

            NetPeer? connectedPeer = null;

            // ─── SERVER SETUP ───
            serverListener.PeerConnectedEvent += (peer) => 
            {
                serverClientConnected = true;
                connectedPeer = peer;
            };

            serverListener.NetworkReceiveEvent += (peer, reader, method) => 
            {
                // Valida o que o cliente mandou
                Assert.Equal(42u, reader.ReadUInt());
                Assert.Equal("Hello Server!", reader.ReadString());

                serverReceivedData = true;

                // Servidor responde
                using var response = new BitBuffer();
                response.AddString("Hello Client!");
                peer.Send(response, DeliveryMethod.ReliableOrdered);
            };

            server.Start(port);
            Assert.True(server.IsRunning);

            // ─── CLIENT SETUP ───
            clientListener.PeerConnectedEvent += (peer) => 
            {
                clientConnected = true;

                // Cliente manda mensagem assim que conecta
                using var writer = new BitBuffer();
                writer.AddUInt(42u);
                writer.AddString("Hello Server!");
                peer.Send(writer, DeliveryMethod.ReliableOrdered);
            };

            clientListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                Assert.Equal("Hello Client!", reader.ReadString());
                clientReceivedData = true;
            };

            client.Connect("127.0.0.1", port);

            // ─── GAME LOOP SIMULATION ───
            int timeoutFrames = 120;
            while (timeoutFrames > 0 && (!serverReceivedData || !clientReceivedData))
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16); // ~60fps
                timeoutFrames--;
            }

            // ─── ASSERTIONS ───
            Assert.True(serverClientConnected, $"[{transportType}] Servidor não registrou a conexão.");
            Assert.True(clientConnected, $"[{transportType}] Cliente não registrou OnConnected.");
            Assert.True(serverReceivedData, $"[{transportType}] Servidor não recebeu os dados.");
            Assert.True(clientReceivedData, $"[{transportType}] Cliente não recebeu os dados.");
            Assert.NotNull(connectedPeer);

            // ─── TEARDOWN ───
            client.Stop();
            
            timeoutFrames = 20;
            bool peerDisconnected = false;
            serverListener.PeerDisconnectedEvent += (peer, reason) => peerDisconnected = true;

            while (timeoutFrames > 0 && !peerDisconnected)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(16);
                timeoutFrames--;
            }

            Assert.True(peerDisconnected, $"[{transportType}] Servidor não registrou o disconnect.");
        }
    }
}
