# NetworkLibrary

**High-Performance UDP/TCP Networking for MMORPGs and Real-Time Games**

Built from scratch in C# with a focus on **zero garbage allocation**, bit-level serialization and massive concurrent throughput.

---

## ✨ Features

- **Dual Transport** — UDP (with reliability layers) and TCP, selectable at runtime
- **Zero Allocation** — `ArrayPool<T>` and `Span<T>` throughout; no GC pressure on hot paths
- **BitBuffer Serialization** — bit-level packing for maximum bandwidth efficiency
- **Reliable Channels** — `Reliable`, `ReliableOrdered`, `Sequenced`, and `Unreliable` delivery methods
- **Packet Fragmentation** — automatic split + reassembly for payloads larger than MTU (~1200 bytes)
- **SO_REUSEPORT** — Linux kernel-level load balancing across multiple receive threads
- **Network Condition Simulator** — built-in latency, jitter and packet loss injection for testing
- **Quantization Helpers** — `BoundedRange`, `SmallestThree` quaternion, `HalfPrecision` float
- **Thread-Safe** — receive threads feed a `ConcurrentQueue`; all game callbacks fire on the poll thread
- **Unity Compatible** — pure C#, no native dependencies; drop the `.dll` into `Assets/Plugins`

---

## ⚡ Stress Test Results

Tested on a single machine with **2000 simultaneous UDP clients**:

| Metric | Result |
|--------|--------|
| Connections | 2,000 / 2,000 (100%) |
| Packets TX | ~1,280,000 / sec |
| Packets RX | ~632,000 / sec |
| Combined Throughput | **~1,900,000 packets / sec** |
| Bandwidth TX | ~104 MB/s |
| RAM Usage | ~300 MB (stable, no growth) |
---

## 📦 Modules

| Module | Description |
|--------|-------------|
| `NetworkLibrary.Transport` | UDP (`NetworkServer`/`NetworkClient`) and TCP (`TcpServer`/`TcpClient`) transports |
| `NetworkLibrary.Serialization` | `BitBuffer` — bit-level read/write with ZigZag, VarLen, compressed float |
| `NetworkLibrary.Quantization` | `BoundedRange`, `HalfPrecision`, `SmallestThree` for position/rotation compression |
| `NetworkLibrary.Compression` | LZ4-style block compression for large payloads |
| `NetworkLibrary.Threading` | Lock-free `MpmcBuffer` and `SpscQueue` for zero-contention packet routing |
| `NetworkLibrary.Buffers` | Custom `ArrayPool<byte>` wrapper |
| `NetworkLibrary.Packets` | `INetPacket` interface and `PacketDispatcher` for typed packet routing |

---

## 🚀 Quick Start

### Server

```csharp
var listener = new EventBasedNetListener();
var server = new NetManager(listener, TransportType.Udp);

listener.PeerConnectedEvent += (peer) =>
{
    Console.WriteLine($"Player connected: {peer.Id}");
};

listener.NetworkReceiveEvent += (peer, reader, method) =>
{
    byte packetId = reader.ReadByte();
    float x = reader.ReadFloat();
    float y = reader.ReadFloat();
    Console.WriteLine($"[{peer.Id}] Move → ({x}, {y})");
};

server.Start(7777);

// Game loop (call ~60 times/sec)
while (true)
{
    server.PollEvents();
    Thread.Sleep(16);
}
```

### Client

```csharp
var listener = new EventBasedNetListener();
var client = new NetManager(listener, TransportType.Udp);

listener.PeerConnectedEvent += (peer) =>
{
    // Send a movement packet
    using var writer = new BitBuffer();
    writer.AddByte(1);          // Packet ID: Move
    writer.AddFloat(150.5f);    // X
    writer.AddFloat(20.0f);     // Y

    peer.Send(writer, DeliveryMethod.Unreliable);
};

client.Connect("127.0.0.1", 7777);

while (true)
{
    client.PollEvents();
    Thread.Sleep(16);
}
```

---

## 🗜️ BitBuffer — Bit-Level Serialization

`BitBuffer` packs values at the exact bit width needed, saving up to **70% bandwidth** versus raw byte writes.

```csharp
using var buffer = new BitBuffer();

// Write
buffer.AddByte(1);              // 8 bits — packet type
buffer.AddUInt(10, 1000);       // 10 bits — HP (0-1000)
buffer.AddUInt(9, 500);         // 9 bits  — Mana (0-500)
buffer.AddUInt(7, 99);          // 7 bits  — Level (0-100)
buffer.AddBool(true);           // 1 bit   — alive flag
buffer.AddFloat(3.14f);         // 32 bits — full precision float
buffer.AddString("PlayerName"); // 8 + N×7 bits — ASCII string

// → Total: 35 bits vs 128 bits uncompressed (72% savings)

// Read
byte type    = buffer.ReadByte();
uint hp      = buffer.ReadUInt(10);
uint mana    = buffer.ReadUInt(9);
uint level   = buffer.ReadUInt(7);
bool alive   = buffer.ReadBool();
float value  = buffer.ReadFloat();
string name  = buffer.ReadString();
```

### Signed Integers (ZigZag Encoding)

```csharp
buffer.AddInt(-100);    // Stored as zigzag: maps negatives to positives
int delta = buffer.ReadInt();
```

### Variable-Length Encoding

```csharp
buffer.AddUIntVar(42);     // Small values (< 128) cost only 8 bits
buffer.AddIntVar(-1000);   // ZigZag + VarLen for signed
```

### Compressed Float

```csharp
// Position X: range -500 to 500, precision 0.01 → saves ~50% vs full float
buffer.AddCompressedFloat(-500f, 500f, 0.01f, position.X);
float x = buffer.ReadCompressedFloat(-500f, 500f, 0.01f);
```

---

## 🎮 Quantization Extensions

### Position (BoundedRange)

```csharp
var range = new BoundedRange(-500f, 500f, 0.1f); // 13 bits per axis
var buffer = new BitBuffer();

buffer.AddVector3(range, x, y, z);
buffer.ReadVector3(range, out float rx, out float ry, out float rz);
```

### Rotation (SmallestThree Quaternion)

```csharp
var buffer = new BitBuffer();

// Compress quaternion from 128 bits → 29 bits (9 bits/component)
buffer.AddQuaternion(qx, qy, qz, qw);
buffer.ReadQuaternion(out float rx, out float ry, out float rz, out float rw);
```

### Half-Precision Float

```csharp
var buffer = new BitBuffer();

buffer.AddHalf(velocity.X);   // 16 bits instead of 32 (50% savings)
float vx = buffer.ReadHalf();
```

### Angle & Normalized

```csharp
buffer.AddAngle(10, 270f);         // 10-bit angle (0.35° precision)
buffer.AddNormalized(8, 0.75f);    // 8-bit normalized (0.0–1.0)

float angle      = buffer.ReadAngle(10);
float normalized = buffer.ReadNormalized(8);
```

---

## 📡 Delivery Methods

| Method | Guarantee | Use Case |
|--------|-----------|----------|
| `Unreliable` | Fire-and-forget | Player position, monster movement |
| `Sequenced` | Only newest packet delivered | Camera rotation |
| `Reliable` | Guaranteed delivery, any order | Damage events, item pickups |
| `ReliableOrdered` | Guaranteed + in-order | Chat messages, quest updates |

```csharp
peer.Send(writer, DeliveryMethod.Unreliable);
peer.Send(writer, DeliveryMethod.ReliableOrdered);
```

---

## 🔌 TCP Mode

Swap `TransportType.Udp` for `TransportType.Tcp` — the API is identical. TCP uses length-prefix framing internally and handles message boundaries automatically.

```csharp
var server = new NetManager(listener, TransportType.Tcp);
server.Start(7777);
```

---

## 🌐 SO_REUSEPORT (Linux Multi-Core)

On Linux, enable kernel-level load balancing across CPU cores:

```csharp
var server = new NetworkServer();
server.EnableReusePort(Environment.ProcessorCount / 2);
server.Start(7777);
```

> Has no effect on Windows — silently falls back to single-socket mode.

---

## 🧪 Chaos Monkey — Network Simulator

```csharp
server.Simulator.Enabled = true;
server.Simulator.LatencyMs = 100;       // 100ms artificial lag
server.Simulator.JitterMs = 20;         // ±20ms jitter
server.Simulator.PacketLossPercent = 5f; // 5% packet loss
```

---

## 🧰 Typed Packet Dispatching

```csharp
// Define a packet
public struct MovePacket : INetPacket
{
    public float X, Y, Z;

    public void Serialize(ref PacketWriter writer)
    {
        writer.Write(X); writer.Write(Y); writer.Write(Z);
    }
    public void Deserialize(ref PacketReader reader)
    {
        X = reader.ReadFloat(); Y = reader.ReadFloat(); Z = reader.ReadFloat();
    }
}

// Register handler
server.Packets.Register<MovePacket>(0x01, (peer, packet) =>
{
    Console.WriteLine($"Player moved to ({packet.X}, {packet.Y}, {packet.Z})");
});

// Send
var move = new MovePacket { X = 100f, Y = 0f, Z = 50f };
peer.Send<MovePacket>(0x01, ref move, DeliveryMethod.Unreliable);
```

---

## 📋 Requirements

- **.NET 6+** (or .NET 10 for full span/half-precision support)
- **Unity 2021+** — add `NetworkLibrary.dll` and `ArcaneShared.dll` to `Assets/Plugins`

---

## 🏗️ Building

```bash
cd Source
dotnet build -c Release
```

## 🧪 Running Tests

```bash
cd Tests
dotnet test
```

## 💥 Running the Stress Test

```bash
cd StressTest
dotnet run -c Release
```

---

## 📄 License

MIT License — use freely in commercial and personal projects.
