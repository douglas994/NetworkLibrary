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

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

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
        public async Task Udp_ReliableOrdered_MixedWholeAndFragmented_WithLoss()
        {
            // Reproduces the enter-world bug: a stream of WHOLE ordered packets interleaved with a FRAGMENTED one
            // (small login packets + a >1194-byte inventory), under loss/jitter that forces reordering+retransmit. The
            // receiver must deliver EACH ordered packet by ITS OWN type. Bug: the whole-packet handler drained the
            // queue and raised interleaved fragments RAW (or vice-versa) → the big packet arrived corrupted = empty bag.
            int testPort = 14009;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();
            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            const int bigSize = 3000;     // > MaxPayloadSize → fragments
            const int smallBefore = 6;    // small whole packets before the big one (like login/spawn/templates)
            const int smallAfter = 6;     // small whole packets after (like currency/companions/spawns)
            int smallSeen = 0;
            bool gotBig = false, bigIntact = true, orderOk = true;
            int nextExpectedSmall = 0;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int tag = reader.ReadInt();
                if (tag >= 0) // small whole packet, carries its index
                {
                    if (tag != nextExpectedSmall) orderOk = false; // smalls must arrive in order, none skipped/corrupted
                    nextExpectedSmall++;
                    smallSeen++;
                }
                else // tag == -1 → the big fragmented packet
                {
                    int len = reader.ReadInt();
                    for (int i = 0; i < len; i++) if (reader.ReadByte() != (byte)(i % 251)) bigIntact = false;
                    if (len == bigSize) gotBig = true;
                }
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                int idx = 0;
                for (int i = 0; i < smallBefore; i++) { using var w = new BitBuffer(); w.AddInt(idx++); peer.Send(w, DeliveryMethod.ReliableOrdered); }
                using (var big = new BitBuffer())
                {
                    big.AddInt(-1); big.AddInt(bigSize);
                    for (int i = 0; i < bigSize; i++) big.AddByte((byte)(i % 251));
                    peer.Send(big, DeliveryMethod.ReliableOrdered);
                }
                for (int i = 0; i < smallAfter; i++) { using var w = new BitBuffer(); w.AddInt(idx++); peer.Send(w, DeliveryMethod.ReliableOrdered); }
            };

            client.Connect("127.0.0.1", testPort);

            // Loss + jitter on BOTH directions → forces the reorder/retransmit that drains fragments via a whole packet's handler.
            server.Simulator!.Enabled = true; server.Simulator.PacketLossPercent = 12f; server.Simulator.LatencyMs = 30; server.Simulator.JitterMs = 25;
            client.Simulator!.Enabled = true; client.Simulator.PacketLossPercent = 12f; client.Simulator.LatencyMs = 30; client.Simulator.JitterMs = 25;

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 25 && !(gotBig && smallSeen == smallBefore + smallAfter))
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.True(orderOk, "Pacotes pequenos chegaram fora de ordem/corrompidos (fragmentos foram entregues crus no lugar deles).");
            Assert.True(gotBig, "O pacote grande fragmentado (misturado com pacotes inteiros) não foi remontado.");
            Assert.True(bigIntact, "O pacote grande remontado veio corrompido.");
            Assert.Equal(smallBefore + smallAfter, smallSeen);
        }

        [Fact]
        public async Task Udp_ServerToClient_MixedWholeAndFragmented_WithLoss()
        {
            // EXACT inventory path: the SERVER sends a mixed ordered stream (small whole login packets + a >1194-byte
            // fragmented inventory + more whole packets) and the CLIENT receives it. Exercises the server fragmenting →
            // client reassembly + ordered delivery under loss/jitter (UDP now backed by LiteNetLib).
            int testPort = 14010;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();
            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            const int bigSize = 3000;
            const int smallBefore = 6, smallAfter = 6;
            int smallSeen = 0, nextExpectedSmall = 0;
            bool gotBig = false, bigIntact = true, orderOk = true;

            var receivedLog = new System.Collections.Generic.List<string>();

            // CLIENT receives (this is the inventory direction).
            clientListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int tag = reader.ReadInt();
                if (tag >= 0) { receivedLog.Add($"SMALL tag={tag} expected={nextExpectedSmall}"); if (tag != nextExpectedSmall) orderOk = false; nextExpectedSmall++; smallSeen++; }
                else
                {
                    int len = reader.ReadInt();
                    receivedLog.Add($"BIG tag={tag} len={len}");
                    for (int i = 0; i < len; i++) if (reader.ReadByte() != (byte)(i % 251)) bigIntact = false;
                    if (len == bigSize) gotBig = true;
                }
            };

            // When the client connects, the SERVER fires PeerConnected with the server-side peer → send the burst to it.
            serverListener.PeerConnectedEvent += (serverPeer) =>
            {
                int idx = 0;
                for (int i = 0; i < smallBefore; i++) { using var w = new BitBuffer(); w.AddInt(idx++); serverPeer.Send(w, DeliveryMethod.ReliableOrdered); }
                using (var big = new BitBuffer())
                {
                    big.AddInt(-1); big.AddInt(bigSize);
                    for (int i = 0; i < bigSize; i++) big.AddByte((byte)(i % 251));
                    serverPeer.Send(big, DeliveryMethod.ReliableOrdered);
                }
                for (int i = 0; i < smallAfter; i++) { using var w = new BitBuffer(); w.AddInt(idx++); serverPeer.Send(w, DeliveryMethod.ReliableOrdered); }
            };

            server.Start(testPort);
            client.Connect("127.0.0.1", testPort);

            server.Simulator!.Enabled = true; server.Simulator.PacketLossPercent = 12f; server.Simulator.LatencyMs = 30; server.Simulator.JitterMs = 25;
            client.Simulator!.Enabled = true; client.Simulator.PacketLossPercent = 12f; client.Simulator.LatencyMs = 30; client.Simulator.JitterMs = 25;

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 25 && !(gotBig && smallSeen == smallBefore + smallAfter))
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            var logStr = string.Join("\n", receivedLog);
            Assert.True(orderOk, $"Server→Client: pacotes pequenos fora de ordem/corrompidos.\nLog:\n{logStr}");
            Assert.True(gotBig, $"Server→Client: inventário fragmentado não foi remontado no cliente.\nLog:\n{logStr}");
            Assert.True(bigIntact, $"Server→Client: inventário remontado corrompido.\nLog:\n{logStr}");
            Assert.Equal(smallBefore + smallAfter, smallSeen);
        }

        [Fact]
        public async Task Udp_ReliableOrdered_StressTest()
        {
            int testPort = 14001;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

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
            using var server = new NetNode(new EventBasedNetListener(), TransportType.Udp) { ConnectionKey = 0xABCDEF01 };

            var clientListener = new EventBasedNetListener();
            using var client = new NetNode(clientListener, TransportType.Udp) { ConnectionKey = 0xDEADBEEF }; // wrong token

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
            using var server = new NetNode(new EventBasedNetListener(), TransportType.Udp) { ConnectionKey = 0xABCDEF01 };

            var clientListener = new EventBasedNetListener();
            using var client = new NetNode(clientListener, TransportType.Udp) { ConnectionKey = 0xABCDEF01 }; // matching token

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

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

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

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

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
        [Fact]
        public async Task Udp_Reliable_FiltersDuplicates()
        {
            int testPort = 14006;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            const int totalMessages = 30;
            var receivedMessages = new System.Collections.Generic.HashSet<int>();
            bool duplicateDetected = false;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int val = reader.ReadInt();
                if (!receivedMessages.Add(val))
                {
                    duplicateDetected = true;
                }
            };

            server.Start(testPort);

            clientListener.PeerConnectedEvent += (peer) =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    using var writer = new BitBuffer();
                    writer.AddInt(i);
                    peer.Send(writer, DeliveryMethod.Reliable);
                }
            };

            client.Connect("127.0.0.1", testPort);

            client.Simulator!.Enabled = true;
            client.Simulator.DuplicatePercent = 50f;

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 5 && receivedMessages.Count < totalMessages)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.False(duplicateDetected, "Recebeu mensagens duplicadas no Reliable!");
            Assert.Equal(totalMessages, receivedMessages.Count);
        }

        [Fact]
        public async Task Udp_ChaosMonkey_ExtremeNetworkConditions()
        {
            int testPort = 14007;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            const int totalMessages = 50;
            int expectedSequence = 0;
            bool testPassed = false;

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int receivedSeq = reader.ReadInt();
                Assert.Equal(expectedSequence, receivedSeq); // Must be strictly ordered
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

            // Caos total: perda, latência variável E duplicação de pacotes!
            client.Simulator!.Enabled = true;
            client.Simulator.PacketLossPercent = 15f;
            client.Simulator.LatencyMs = 50;
            client.Simulator.JitterMs = 20;
            client.Simulator.DuplicatePercent = 25f;

            server.Simulator!.Enabled = true;
            server.Simulator.PacketLossPercent = 15f;
            server.Simulator.LatencyMs = 50;
            server.Simulator.JitterMs = 20;

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 25 && !testPassed)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(10);
            }

            Assert.True(testPassed, $"Chaos monkey failed! Recebeu apenas {expectedSequence}/{totalMessages}");
        }
        /// <summary>
        /// Reproduces the inventory bug: server sends a large (fragmented) ReliableOrdered
        /// packet to the client under packet loss. Before the fix, retransmitted fragments
        /// were sent as ReliableOrdered (not ReliableOrderedFragment), so the client treated
        /// the raw fragment bytes as game data instead of reassembling them.
        /// </summary>
        [Fact]
        public async Task Udp_FragmentedReliableOrdered_ServerToClient_SurvivesPacketLoss()
        {
            int testPort = 14008;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            // 5000 bytes = ~5 fragments (MTU ~1200), simulating a large inventory payload
            const int dataSize = 5000;
            bool clientReceivedData = false;
            byte[]? receivedData = null;

            clientListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int length = reader.ReadInt();
                receivedData = new byte[length];
                for (int i = 0; i < length; i++)
                {
                    receivedData[i] = reader.ReadByte();
                }
                clientReceivedData = true;
            };

            server.Start(testPort);

            NetPeer? serverPeer = null;
            serverListener.PeerConnectedEvent += (peer) =>
            {
                serverPeer = peer;
            };

            client.Connect("127.0.0.1", testPort);

            // Wait for connection
            var connectDeadline = System.Diagnostics.Stopwatch.StartNew();
            while (connectDeadline.Elapsed.TotalSeconds < 3 && serverPeer == null)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.NotNull(serverPeer);

            // Enable packet loss on the server→client path (simulates real network)
            server.Simulator!.Enabled = true;
            server.Simulator.PacketLossPercent = 20f;

            // Server sends a large packet (requires fragmentation) to the client
            using var writer = new BitBuffer();
            writer.AddInt(dataSize);
            for (int i = 0; i < dataSize; i++)
            {
                writer.AddByte((byte)(i % 251)); // prime pattern for integrity
            }

            serverPeer.Send(writer, DeliveryMethod.ReliableOrdered);

            // Poll until client receives or timeout
            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 20 && !clientReceivedData)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.True(clientReceivedData, "Client nunca recebeu o pacote fragmentado sob perda!");
            Assert.NotNull(receivedData);
            Assert.Equal(dataSize, receivedData!.Length);

            for (int i = 0; i < dataSize; i++)
            {
                Assert.Equal((byte)(i % 251), receivedData[i]);
            }
        }

        /// <summary>
        /// Stress test: bidirectional fragmented + small ReliableOrdered under extreme loss + duplication.
        /// This is the worst case: server sends large inventory AND small updates interleaved,
        /// with both loss and duplication active.
        /// </summary>
        [Fact]
        public async Task Udp_FragmentedReliableOrdered_Bidirectional_ChaosMonkey()
        {
            int testPort = 14009;
            var serverListener = new EventBasedNetListener();
            var clientListener = new EventBasedNetListener();

            using var server = new NetNode(serverListener, TransportType.Udp);
            using var client = new NetNode(clientListener, TransportType.Udp);

            const int dataSize = 3000; // large enough for fragmentation
            bool clientReceivedLarge = false;
            bool serverReceivedLarge = false;

            clientListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int length = reader.ReadInt();
                if (length == dataSize)
                {
                    // Verify integrity
                    bool valid = true;
                    for (int i = 0; i < length; i++)
                    {
                        if (reader.ReadByte() != (byte)(i % 199))
                        {
                            valid = false;
                            break;
                        }
                    }
                    if (valid) clientReceivedLarge = true;
                }
            };

            serverListener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                int length = reader.ReadInt();
                if (length == dataSize)
                {
                    bool valid = true;
                    for (int i = 0; i < length; i++)
                    {
                        if (reader.ReadByte() != (byte)(i % 197))
                        {
                            valid = false;
                            break;
                        }
                    }
                    if (valid) serverReceivedLarge = true;
                }
            };

            server.Start(testPort);

            NetPeer? serverPeerRef = null;
            serverListener.PeerConnectedEvent += (peer) => serverPeerRef = peer;

            clientListener.PeerConnectedEvent += (peer) =>
            {
                // Client sends large packet to server
                using var w = new BitBuffer();
                w.AddInt(dataSize);
                for (int i = 0; i < dataSize; i++)
                    w.AddByte((byte)(i % 197));
                peer.Send(w, DeliveryMethod.ReliableOrdered);
            };

            client.Connect("127.0.0.1", testPort);

            // Wait for connection
            var connectDeadline = System.Diagnostics.Stopwatch.StartNew();
            while (connectDeadline.Elapsed.TotalSeconds < 3 && serverPeerRef == null)
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.NotNull(serverPeerRef);

            // Enable chaos on both sides
            server.Simulator!.Enabled = true;
            server.Simulator.PacketLossPercent = 15f;
            server.Simulator.DuplicatePercent = 20f;
            client.Simulator!.Enabled = true;
            client.Simulator.PacketLossPercent = 15f;
            client.Simulator.DuplicatePercent = 20f;

            // Server sends large packet to client
            using var serverWriter = new BitBuffer();
            serverWriter.AddInt(dataSize);
            for (int i = 0; i < dataSize; i++)
                serverWriter.AddByte((byte)(i % 199));
            serverPeerRef.Send(serverWriter, DeliveryMethod.ReliableOrdered);

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (deadline.Elapsed.TotalSeconds < 25 && (!clientReceivedLarge || !serverReceivedLarge))
            {
                server.PollEvents();
                client.PollEvents();
                await Task.Delay(5);
            }

            Assert.True(clientReceivedLarge, "Client não recebeu o pacote fragmentado do servidor sob chaos!");
            Assert.True(serverReceivedLarge, "Server não recebeu o pacote fragmentado do cliente sob chaos!");
        }
    }
}
