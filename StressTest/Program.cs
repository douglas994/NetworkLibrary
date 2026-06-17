using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using NetworkLibrary;
using NetworkLibrary.Serialization;
using NetworkLibrary.Transport;
using NetworkLibrary.Quantization;

namespace StressTest
{
    class Program
    {
        // === CONFIGURATION ===
        const int Port = 7777;
        const int NumClients = 2000;
        const int NumMonsters = 10000;
        const int TicksPerSecond = 20; // 50ms interval
        const int ClientPacketsPerTick = 15;  // Each client sends 15 unreliable pkts/tick → 15 × 2000 × 20 = 600k TX client/s
        const int ServerPacketsPerClientPerTick = 16; // Server sends 16 AoI pkts/tick/client → 16 × 2000 × 20 = 640k TX server/s
        // Grand total target: 1.240.000 packets/s (both directions combined)
        const TransportType Protocol = TransportType.Udp; // Change to Tcp to compare!

        // === METRICS ===
        static int totalConnections = 0;
        static int packetsSent = 0;
        static int packetsReceived = 0;
        static long bytesReceived = 0;
        static long bytesSent = 0;

        static readonly BoundedRange RotRange = new BoundedRange(0f, 360f, 0.1f);
        
        // Track connected peers on the server for broadcasting
        static List<NetPeer> serverPeers = new List<NetPeer>();
        static readonly object serverPeersLock = new object();

        static void Main(string[] args)
        {
            // Memory measurement mode: NL_MEASURE_PEERS=1 — isolates the per-peer server-side cost.
            if (Environment.GetEnvironmentVariable("NL_MEASURE_PEERS") == "1")
            {
                MeasurePeerMemory();
                return;
            }

            // Reliability-under-loss mode: validates the ReliableOrdered channel survives heavy loss + many peers
            // with NO permanent stall (the bug fixed 2026-06-15). Run: NL_RELIABILITY=1 dotnet run -c Release
            if (Environment.GetEnvironmentVariable("NL_RELIABILITY") == "1")
            {
                RunReliabilityTest();
                return;
            }

            Console.WriteLine("=====================================");
            Console.WriteLine($"   MMORPG STRESS TEST ({Protocol})   ");
            Console.WriteLine("=====================================");
            Console.WriteLine($"Clients: {NumClients} | Ticks: {TicksPerSecond}/s");

            // 1. Start the Server
            var server = StartServer();
            Console.WriteLine("Server started on port " + Port);

            // 2. Start the Clients
            Console.WriteLine("Spawning clients...");
            var clients = new List<ClientData>(NumClients);
            for (int i = 0; i < NumClients; i++)
            {
                clients.Add(StartClient(i));
                // Optional: slight delay to avoid massive login storm instantly
                if (i % 100 == 0) Thread.Sleep(10);
            }

            Console.WriteLine("All clients spawned!");

            // 3. Start the Dashboard UI
            Task.Run(DashboardLoop);

            // 4. Start the Client Movement Loop
            Task.Run(() => ClientMovementLoop(clients));

            // 5. Start the Server Monster / AoI Loop
            Task.Run(ServerMonsterLoop);

            // 6. Run the Server Poll Loop (Main Thread)
            while (true)
            {
                server.PollEvents();

                // Also poll clients here to receive ACKs if TCP, or Unreliable echoes if UDP
                for(int i = 0; i < clients.Count; i++)
                {
                    clients[i].Manager.PollEvents();
                }

                Thread.Sleep(1); // Small sleep to prevent 100% CPU lock
            }
        }

        static void MeasurePeerMemory()
        {
            Console.WriteLine("=== PER-PEER MEMORY MEASUREMENT (server-side NetworkPeer) ===");

            // Warm up: force JIT/type init so it doesn't pollute the measurement.
            var warm = new NetworkLibrary.Transport.NetworkPeer();
            GC.KeepAlive(warm);

            // GetAllocatedBytesForCurrentThread counts allocation bytes EXACTLY (not quantized by
            // GC heap segments like GetTotalMemory), so per-peer cost is precise.
            foreach (int n in new[] { 1000, 5000, 10000 })
            {
                long before = GC.GetAllocatedBytesForCurrentThread();

                var peers = new NetworkLibrary.Transport.NetworkPeer[n];
                for (int i = 0; i < n; i++)
                    peers[i] = new NetworkLibrary.Transport.NetworkPeer();

                long after = GC.GetAllocatedBytesForCurrentThread();
                GC.KeepAlive(peers);

                long total = after - before;
                Console.WriteLine($"{n,6} peers: {total / 1024.0 / 1024.0,7:F2} MB total  |  {(double)total / n,7:F0} bytes/peer");
            }
            Console.WriteLine("=== (idle/baseline cost — on-demand reliable Data buffers are pooled separately) ===");

            // Now measure a full NetworkClient object (what the StressTest co-locates 2000 of).
            var warmC = new NetworkLibrary.Transport.NetworkClient();
            GC.KeepAlive(warmC);
            Console.WriteLine("\n=== PER-CLIENT MEMORY (object only — does NOT include the OS thread each Connect() spawns) ===");
            foreach (int n in new[] { 1000, 2000 })
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                var clients = new NetworkLibrary.Transport.NetworkClient[n];
                for (int i = 0; i < n; i++)
                    clients[i] = new NetworkLibrary.Transport.NetworkClient();
                long after = GC.GetAllocatedBytesForCurrentThread();
                GC.KeepAlive(clients);
                long total = after - before;
                Console.WriteLine($"{n,6} clients: {total / 1024.0 / 1024.0,7:F2} MB total  |  {(double)total / n,7:F0} bytes/client");
            }
        }

        // === RELIABILITY-UNDER-LOSS TEST (NL_RELIABILITY=1) ===
        // The server streams a numbered ReliableOrdered sequence to EVERY client under heavy packet loss; each client
        // must receive the WHOLE sequence strictly in order (retransmits recover every loss) with NO permanent stall.
        // Clients send unreliable "movement" so ACKs piggyback back, exactly like the game. Proves the reliable-channel
        // fix (never give up / window 256) holds under stress, and gives a keep-vs-swap-transport data point.
        sealed class RelState { public int Expected; public int Received; public int OutOfOrder; public long LastProgress; public NetPeer? Peer; }

        static void RunReliabilityTest()
        {
            int RelClients = int.TryParse(Environment.GetEnvironmentVariable("NL_REL_CLIENTS"), out var rc) ? rc : 300;
            const int DurationSec = 30;
            int ServerOrderedPerSec = int.TryParse(Environment.GetEnvironmentVariable("NL_REL_RATE"), out var rr) ? rr : 10;
            const int ClientMovePerSec = 15;     // unreliable movement (carries ACKs back), per client
            float Loss = float.TryParse(Environment.GetEnvironmentVariable("NL_REL_LOSS"), out var ls) ? ls : 8f;
            int Latency = 80, Jitter = 30;

            Console.WriteLine("=====================================");
            Console.WriteLine("  RELIABILITY UNDER LOSS (ordered)   ");
            Console.WriteLine($"  {RelClients} clients | {Loss}% loss | {Latency}±{Jitter}ms | {DurationSec}s");
            Console.WriteLine("=====================================");

            var srvPeers = new List<NetPeer>();
            object srvLock = new();
            var peerSeq = new System.Collections.Concurrent.ConcurrentDictionary<NetPeer, int>();
            var srvListener = new EventBasedNetListener();
            srvListener.PeerConnectedEvent += p => { peerSeq[p] = 0; lock (srvLock) srvPeers.Add(p); };
            srvListener.PeerDisconnectedEvent += (p, r) => { lock (srvLock) srvPeers.Remove(p); };
            srvListener.NetworkReceiveEvent += (p, reader, m) => { }; // client movement/acks: nothing to verify here
            var server = new NetManager(srvListener, TransportType.Udp);
            if (server.Simulator != null) { server.Simulator.Enabled = true; server.Simulator.PacketLossPercent = Loss; server.Simulator.LatencyMs = Latency; server.Simulator.JitterMs = Jitter; }
            server.Start(Port, 1);

            var states = new RelState[RelClients];
            var clients = new NetManager[RelClients];
            for (int i = 0; i < RelClients; i++)
            {
                var st = new RelState { LastProgress = Stopwatch.GetTimestamp() };
                states[i] = st;
                var cl = new EventBasedNetListener();
                cl.PeerConnectedEvent += p => st.Peer = p;
                cl.NetworkReceiveEvent += (p, reader, m) =>
                {
                    if (reader.ReadByte() != 3) return;        // only the ordered stream
                    int seq = reader.ReadInt();
                    if (seq == st.Expected) { st.Expected++; st.Received++; st.LastProgress = Stopwatch.GetTimestamp(); }
                    else if (seq > st.Expected) st.OutOfOrder++; // ordered channel must NEVER skip ahead
                    // seq < Expected = duplicate → ignore
                };
                var c = new NetManager(cl, TransportType.Udp);
                if (c.Simulator != null) { c.Simulator.Enabled = true; c.Simulator.PacketLossPercent = Loss; c.Simulator.LatencyMs = Latency; c.Simulator.JitterMs = Jitter; }
                c.Connect("127.0.0.1", Port);
                clients[i] = c;
                if (i % 50 == 0) Thread.Sleep(10); // avoid a connect storm
            }

            long freq = Stopwatch.Frequency;
            long connectDeadline = Stopwatch.GetTimestamp() + 15 * freq;
            while (Stopwatch.GetTimestamp() < connectDeadline)
            {
                server.PollEvents();
                for (int i = 0; i < clients.Length; i++) clients[i].PollEvents();
                int n; lock (srvLock) n = srvPeers.Count;
                if (n >= RelClients) break;
                Thread.Sleep(5);
            }
            int connected; lock (srvLock) connected = srvPeers.Count;
            Console.WriteLine($"Connected: {connected}/{RelClients}");

            long start = Stopwatch.GetTimestamp();
            long end = start + DurationSec * freq;
            long sendInterval = ServerOrderedPerSec > 0 ? freq / ServerOrderedPerSec : long.MaxValue, moveInterval = freq / ClientMovePerSec;
            long nextSend = start, nextMove = start, totalSent = 0;
            long lastReport = start;
            while (Stopwatch.GetTimestamp() < end)
            {
                server.PollEvents();
                for (int i = 0; i < clients.Length; i++) clients[i].PollEvents();
                long now = Stopwatch.GetTimestamp();

                if (now >= nextSend)
                {
                    nextSend += sendInterval;
                    NetPeer[] snap; lock (srvLock) snap = srvPeers.ToArray();
                    for (int i = 0; i < snap.Length; i++)
                    {
                        var p = snap[i];
                        int seq = peerSeq.TryGetValue(p, out var s) ? s : 0;
                        var w = new BitBuffer(); w.AddByte(3); w.AddInt(seq); p.Send(w, DeliveryMethod.ReliableOrdered); w.Dispose();
                        peerSeq[p] = seq + 1; totalSent++;
                    }
                }
                if (now >= nextMove)
                {
                    nextMove += moveInterval;
                    for (int i = 0; i < clients.Length; i++) { var p = states[i].Peer; if (p == null) continue; var w = new BitBuffer(); w.AddByte(1); w.AddInt(i); p.Send(w, DeliveryMethod.Unreliable); w.Dispose(); }
                }
                if (now - lastReport >= 5 * freq)
                {
                    lastReport = now;
                    long rec = 0; for (int i = 0; i < states.Length; i++) rec += states[i].Received;
                    Console.WriteLine($"  t={(now - start) / freq,2}s  sent={totalSent}  received={rec}");
                }
                Thread.Sleep(1);
            }

            // Drain: stop the ordered stream, keep polling + acking so retransmits land.
            Console.WriteLine("Draining retransmits (8s)...");
            long drainEnd = Stopwatch.GetTimestamp() + 8 * freq;
            while (Stopwatch.GetTimestamp() < drainEnd)
            {
                server.PollEvents();
                for (int i = 0; i < clients.Length; i++) clients[i].PollEvents();
                long now = Stopwatch.GetTimestamp();
                if (now >= nextMove)
                {
                    nextMove += moveInterval;
                    for (int i = 0; i < clients.Length; i++) { var p = states[i].Peer; if (p == null) continue; var w = new BitBuffer(); w.AddByte(1); w.AddInt(i); p.Send(w, DeliveryMethod.Unreliable); w.Dispose(); }
                }
                Thread.Sleep(1);
            }

            long totalReceived = 0; int ooo = 0, stalled = 0, minRec = int.MaxValue, maxRec = 0;
            double avgPerPeer = connected > 0 ? (double)totalSent / connected : 0;
            for (int i = 0; i < states.Length; i++)
            {
                var st = states[i];
                totalReceived += st.Received; ooo += st.OutOfOrder;
                if (st.Received < minRec) minRec = st.Received;
                if (st.Received > maxRec) maxRec = st.Received;
                if (st.Received < avgPerPeer * 0.90) stalled++;
            }
            double deliveryPct = totalSent > 0 ? 100.0 * totalReceived / totalSent : 0;
            Console.WriteLine("───── RESULT ─────");
            Console.WriteLine($"Ordered sent (total):     {totalSent}");
            Console.WriteLine($"Ordered received (total): {totalReceived}  ({deliveryPct:F2}%)");
            Console.WriteLine($"Per-client received:      min {minRec} / avg {avgPerPeer:F0} / max {maxRec}");
            Console.WriteLine($"Out-of-order (must be 0): {ooo}");
            Console.WriteLine($"Stalled clients (<90%):   {stalled}");
            bool pass = ooo == 0 && stalled == 0 && deliveryPct >= 99.5;
            Console.WriteLine(pass ? "VERDICT: PASS ✅ reliable-ordered survived loss+load, no stall"
                                   : "VERDICT: FAIL ❌ — stall/loss/order issue, investigate");
        }

        static NetManager StartServer()
        {
            var listener = new EventBasedNetListener();

            listener.PeerConnectedEvent += (peer) => 
            {
                Interlocked.Increment(ref totalConnections);
                lock(serverPeersLock) serverPeers.Add(peer);
            };
            listener.PeerDisconnectedEvent += (peer, reason) => 
            {
                Interlocked.Decrement(ref totalConnections);
                lock(serverPeersLock) serverPeers.Remove(peer);
            };

            listener.NetworkReceiveEvent += (peer, reader, method) =>
            {
                Interlocked.Increment(ref packetsReceived);
                Interlocked.Add(ref bytesReceived, reader.Length); // BitBuffer.Length is already in bytes

                // Read Movement Data
                byte type = reader.ReadByte();
                if (type == 1) // Movement
                {
                    // Ignore
                }
                else if (type == 2) // Reliable Echo
                {
                    // Server echos back reliably to test Server Retransmission!
                    using var reply = new BitBuffer();
                    reply.AddByte(2);
                    peer.Send(reply, DeliveryMethod.Reliable);
                    Interlocked.Increment(ref packetsSent);
                    Interlocked.Add(ref bytesSent, reply.Length);
                }
            };

            var server = new NetManager(listener, Protocol);
            
            // Enable Chaos Monkey (Simulate Bad Network)
            if (server.Simulator != null)
            {
                server.Simulator.Enabled = false; // Disable for Raw Throughput test
                server.Simulator.LatencyMs = 100;
                server.Simulator.JitterMs = 20;
                server.Simulator.PacketLossPercent = 5f; // 5% packet loss for robust testing
            }

            // Parallel receive: configurable via NL_RECV_THREADS env var (default 1) for A/B testing.
            int recvThreads = int.TryParse(Environment.GetEnvironmentVariable("NL_RECV_THREADS"), out var rt) ? rt : 1;
            server.Start(Port, recvThreads);
            Console.WriteLine($"Receive threads: {recvThreads}");
            return server;
        }

        class ClientData
        {
            public NetManager Manager { get; set; } = null!;
            public NetPeer? Peer { get; set; }
        }

        static ClientData StartClient(int id)
        {
            var listener = new EventBasedNetListener();
            var data = new ClientData();

            listener.PeerConnectedEvent += (peer) => { data.Peer = peer; };

            // Ignore receive on client for this pure TX stress test
            listener.NetworkReceiveEvent += (peer, reader, method) => { };

            var client = new NetManager(listener, Protocol);
            client.Connect("127.0.0.1", Port);
            data.Manager = client;
            return data;
        }

        static void ClientMovementLoop(List<ClientData> clients)
        {
            int sleepTime = 1000 / TicksPerSecond;

            long lastReliableSent = Stopwatch.GetTimestamp();

            while (true)
            {
                long start = Stopwatch.GetTimestamp();
                bool sendReliable = (start - lastReliableSent) > Stopwatch.Frequency;

                // Parallel across all clients — each thread has its own Random to avoid contention
                Parallel.For(0, clients.Count, () => new Random(Thread.CurrentThread.ManagedThreadId), (i, state, rand) =>
                {
                    var peer = clients[i].Peer;
                    if (peer != null)
                    {
                        for (int burst = 0; burst < ClientPacketsPerTick; burst++)
                        {
                            using var writer = new BitBuffer();
                            writer.AddByte(1);
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 1000f)));
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 10f)));
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 1000f)));
                            writer.AddUShort((ushort)RotRange.Quantize((float)(rand.NextDouble() * 360f)));
                            peer.Send(writer, DeliveryMethod.Unreliable);
                            Interlocked.Increment(ref packetsSent);
                            Interlocked.Add(ref bytesSent, writer.Length);
                        }

                        if (sendReliable)
                        {
                            using var relWriter = new BitBuffer();
                            relWriter.AddByte(2);
                            relWriter.AddInt(123456);
                            peer.Send(relWriter, DeliveryMethod.Reliable);
                            Interlocked.Increment(ref packetsSent);
                            Interlocked.Add(ref bytesSent, relWriter.Length);
                        }
                    }
                    return rand;
                }, _ => { });

                if (sendReliable)
                    lastReliableSent = start;

                long elapsedTicks = Stopwatch.GetTimestamp() - start;
                int elapsedMs = (int)(elapsedTicks * 1000 / Stopwatch.Frequency);

                int sleepTimeMs = sleepTime - elapsedMs;
                if (sleepTimeMs > 0)
                    Thread.Sleep(sleepTimeMs);
                else
                    Thread.Yield();
            }
        }

        static void ServerMonsterLoop()
        {
            int sleepTime = 1000 / TicksPerSecond;
            const int MonstersPerClient = 20;

            while (true)
            {
                long start = Stopwatch.GetTimestamp();

                // 1. Monster AI would run here (no-op in benchmark)
                // Thread.SpinWait removed — not needed for pure throughput test

                // 2. Snapshot the peer list outside the lock
                NetPeer[] snapshot;
                lock (serverPeersLock)
                {
                    snapshot = serverPeers.ToArray();
                }

                // 3. Broadcast AoI in parallel — each thread has its own Random
                Parallel.For(0, snapshot.Length, () => new Random(Thread.CurrentThread.ManagedThreadId), (i, state, rand) =>
                {
                    var peer = snapshot[i];
                    for (int burst = 0; burst < ServerPacketsPerClientPerTick; burst++)
                    {
                        using var writer = new BitBuffer();
                        writer.AddByte(3);
                        writer.AddByte(MonstersPerClient);
                        for (int m = 0; m < MonstersPerClient; m++)
                        {
                            writer.AddUShort((ushort)rand.Next(10000));
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 1000f)));
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 10f)));
                            writer.AddUShort(HalfPrecision.Quantize((float)(rand.NextDouble() * 1000f)));
                        }
                        peer.Send(writer, DeliveryMethod.Unreliable);
                        Interlocked.Increment(ref packetsSent);
                        Interlocked.Add(ref bytesSent, writer.Length);
                    }
                    return rand;
                }, _ => { });

                long elapsedTicks = Stopwatch.GetTimestamp() - start;
                int elapsedMs = (int)(elapsedTicks * 1000 / Stopwatch.Frequency);

                int sleepTimeMs = sleepTime - elapsedMs;
                if (sleepTimeMs > 0)
                    Thread.Sleep(sleepTimeMs);
                else
                    Thread.Yield();
            }
        }
        static void DashboardLoop()
        {
            var sw = Stopwatch.StartNew();
            int lastTx = 0;
            int lastRx = 0;
            long lastBytesRx = 0;
            long lastBytesTx = 0;

            while (true)
            {
                Thread.Sleep(1000);

                int currentTx = packetsSent;
                int currentRx = packetsReceived;
                long currentBytesRx = bytesReceived;
                long currentBytesTx = bytesSent;

                int txPerSec = currentTx - lastTx;
                int rxPerSec = currentRx - lastRx;
                
                double rxMb = (currentBytesRx - lastBytesRx) / 1024.0 / 1024.0;
                double txMb = (currentBytesTx - lastBytesTx) / 1024.0 / 1024.0;

                long ramMb = Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;

                lastTx = currentTx;
                lastRx = currentRx;
                lastBytesRx = currentBytesRx;
                lastBytesTx = currentBytesTx;

                // Print dashboard
                Console.WriteLine("-------------------------------------");
                Console.WriteLine($" Connections Active: {totalConnections} / {NumClients}     ");
                Console.WriteLine($" Memory Usage (RAM): {ramMb} MB           ");
                Console.WriteLine("-------------------------------------");
                Console.WriteLine($" Packets TX: {txPerSec} / sec          ");
                Console.WriteLine($" Packets RX: {rxPerSec} / sec          ");
                Console.WriteLine("-------------------------------------");
                Console.WriteLine($" Bandwidth TX: {txMb:F2} MB/s           ");
                Console.WriteLine($" Bandwidth RX: {rxMb:F2} MB/s           ");
                Console.WriteLine("-------------------------------------");
            }
        }

        static void RunSerializationBenchmark()
        {
            Console.WriteLine("=====================================");
            Console.WriteLine("   BITBUFFER SERIALIZATION BENCHMARK ");
            Console.WriteLine("=====================================");
            
            int iterations = 10_000_000; // 10 million packets
            Console.WriteLine($"Running {iterations:N0} iterations...");

            var sw = Stopwatch.StartNew();

            for (int i = 0; i < iterations; i++)
            {
                // Serialize
                using var writer = new BitBuffer();
                writer.AddByte(1); // Packet ID
                writer.AddUInt(10045); // Entity ID
                writer.AddUShort(HalfPrecision.Quantize(150.5f)); // X
                writer.AddUShort(HalfPrecision.Quantize(20.1f));  // Y
                writer.AddUShort(HalfPrecision.Quantize(300.9f)); // Z
                writer.AddUShort((ushort)RotRange.Quantize(180.5f)); // Rot
                writer.AddUShort(100); // Health
                writer.AddByte(5); // State

                // Deserialize (simulating receive)
                byte[] data = writer.ToArray();
                using var reader = new BitBuffer();
                reader.FromArray(data, data.Length);
                byte id = reader.ReadByte();
                uint entityId = reader.ReadUInt();
                float x = HalfPrecision.Dequantize(reader.ReadUShort());
                float y = HalfPrecision.Dequantize(reader.ReadUShort());
                float z = HalfPrecision.Dequantize(reader.ReadUShort());
                float rot = RotRange.Dequantize(reader.ReadUShort());
                ushort hp = reader.ReadUShort();
                byte state = reader.ReadByte();
            }

            sw.Stop();
            Console.WriteLine($"Total Time: {sw.ElapsedMilliseconds} ms");
            Console.WriteLine($"Ops / sec : {(iterations / sw.Elapsed.TotalSeconds):N0} (Write + Read)");
            Console.WriteLine("=====================================");
        }
    }
}
