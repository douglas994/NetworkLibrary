using System;
using Xunit;
using NetworkLibrary.Serialization;
using NetworkLibrary.Quantization;

namespace NetworkLibrary.Tests
{
    public class BitBufferTests
    {
        // ═══════════════════════════════════════════════════════
        //  BASIC TYPES
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void Bool_RoundTrip()
        {
            using var buffer = new BitBuffer();
            buffer.AddBool(true);
            buffer.AddBool(false);
            buffer.AddBool(true);

            Assert.True(buffer.ReadBool());
            Assert.False(buffer.ReadBool());
            Assert.True(buffer.ReadBool());
            Assert.True(buffer.IsFinished);
        }

        [Fact]
        public void Byte_RoundTrip()
        {
            using var buffer = new BitBuffer();
            buffer.AddByte(0);
            buffer.AddByte(127);
            buffer.AddByte(255);

            Assert.Equal(0, buffer.ReadByte());
            Assert.Equal(127, buffer.ReadByte());
            Assert.Equal(255, buffer.ReadByte());
        }

        [Fact]
        public void UShort_RoundTrip()
        {
            using var buffer = new BitBuffer();
            buffer.AddUShort(0);
            buffer.AddUShort(1000);
            buffer.AddUShort(ushort.MaxValue);

            Assert.Equal((ushort)0, buffer.ReadUShort());
            Assert.Equal((ushort)1000, buffer.ReadUShort());
            Assert.Equal(ushort.MaxValue, buffer.ReadUShort());
        }

        [Fact]
        public void UInt_FullRoundTrip()
        {
            using var buffer = new BitBuffer();
            buffer.AddUInt(0u);
            buffer.AddUInt(42u);
            buffer.AddUInt(uint.MaxValue);

            Assert.Equal(0u, buffer.ReadUInt());
            Assert.Equal(42u, buffer.ReadUInt());
            Assert.Equal(uint.MaxValue, buffer.ReadUInt());
        }

        [Fact]
        public void Float_RoundTrip()
        {
            using var buffer = new BitBuffer();
            buffer.AddFloat(0f);
            buffer.AddFloat(3.14159f);
            buffer.AddFloat(-999.5f);
            buffer.AddFloat(float.MaxValue);

            Assert.Equal(0f, buffer.ReadFloat());
            Assert.Equal(3.14159f, buffer.ReadFloat());
            Assert.Equal(-999.5f, buffer.ReadFloat());
            Assert.Equal(float.MaxValue, buffer.ReadFloat());
        }

        // ═══════════════════════════════════════════════════════
        //  BIT-PACKING
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void BitPacking_CustomBitWidths()
        {
            using var buffer = new BitBuffer();

            // HP: 0-1000 = 10 bits
            buffer.AddUInt(10, 1000);
            // Mana: 0-500 = 9 bits
            buffer.AddUInt(9, 500);
            // Level: 0-100 = 7 bits
            buffer.AddUInt(7, 99);
            // Alive: 1 bit
            buffer.AddBool(true);

            Assert.Equal(1000u, buffer.ReadUInt(10));
            Assert.Equal(500u, buffer.ReadUInt(9));
            Assert.Equal(99u, buffer.ReadUInt(7));
            Assert.True(buffer.ReadBool());

            // Total: 10 + 9 + 7 + 1 = 27 bits (vs 128 bits without packing = 79% savings)
            Assert.Equal(27, buffer.BitsWritten);
        }

        [Fact]
        public void BitPacking_EdgeCases()
        {
            using var buffer = new BitBuffer();

            // 1 bit
            buffer.AddRaw(1, 1);
            Assert.Equal(1u, buffer.ReadRaw(1));

            buffer.Clear();

            // 32 bits
            buffer.AddRaw(32, uint.MaxValue);
            Assert.Equal(uint.MaxValue, buffer.ReadRaw(32));
        }

        // ═══════════════════════════════════════════════════════
        //  ZIGZAG ENCODING (signed integers)
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void ZigZag_SignedIntegers()
        {
            using var buffer = new BitBuffer();

            buffer.AddInt(0);
            buffer.AddInt(1);
            buffer.AddInt(-1);
            buffer.AddInt(100);
            buffer.AddInt(-100);
            buffer.AddInt(int.MaxValue);
            buffer.AddInt(int.MinValue);

            Assert.Equal(0, buffer.ReadInt());
            Assert.Equal(1, buffer.ReadInt());
            Assert.Equal(-1, buffer.ReadInt());
            Assert.Equal(100, buffer.ReadInt());
            Assert.Equal(-100, buffer.ReadInt());
            Assert.Equal(int.MaxValue, buffer.ReadInt());
            Assert.Equal(int.MinValue, buffer.ReadInt());
        }

        [Fact]
        public void ZigZag_SmallRange()
        {
            using var buffer = new BitBuffer();

            // -50 to 50 needs 7 bits with ZigZag
            buffer.AddInt(8, 50);
            buffer.AddInt(8, -50);
            buffer.AddInt(8, 0);

            Assert.Equal(50, buffer.ReadInt(8));
            Assert.Equal(-50, buffer.ReadInt(8));
            Assert.Equal(0, buffer.ReadInt(8));
        }

        // ═══════════════════════════════════════════════════════
        //  VARIABLE-LENGTH ENCODING
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void VarLength_SmallValues()
        {
            using var buffer = new BitBuffer();

            buffer.AddUIntVar(0);
            buffer.AddUIntVar(127);
            buffer.AddUIntVar(128);
            buffer.AddUIntVar(16383);
            buffer.AddUIntVar(uint.MaxValue);

            Assert.Equal(0u, buffer.ReadUIntVar());
            Assert.Equal(127u, buffer.ReadUIntVar());
            Assert.Equal(128u, buffer.ReadUIntVar());
            Assert.Equal(16383u, buffer.ReadUIntVar());
            Assert.Equal(uint.MaxValue, buffer.ReadUIntVar());
        }

        [Fact]
        public void VarLength_SmallValuesUseFewBits()
        {
            using var buffer = new BitBuffer();
            buffer.AddUIntVar(42);

            // Value 42 < 128, so should use only 8 bits (7 data + 1 continuation)
            Assert.Equal(8, buffer.BitsWritten);
        }

        [Fact]
        public void VarLength_SignedIntegers()
        {
            using var buffer = new BitBuffer();

            buffer.AddIntVar(0);
            buffer.AddIntVar(1);
            buffer.AddIntVar(-1);
            buffer.AddIntVar(1000);
            buffer.AddIntVar(-1000);

            Assert.Equal(0, buffer.ReadIntVar());
            Assert.Equal(1, buffer.ReadIntVar());
            Assert.Equal(-1, buffer.ReadIntVar());
            Assert.Equal(1000, buffer.ReadIntVar());
            Assert.Equal(-1000, buffer.ReadIntVar());
        }

        // ═══════════════════════════════════════════════════════
        //  STRING
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void String_ASCII()
        {
            using var buffer = new BitBuffer();

            buffer.AddString("Hello, MMORPG!");
            buffer.AddString("");
            buffer.AddString("A");

            Assert.Equal("Hello, MMORPG!", buffer.ReadString());
            Assert.Equal("", buffer.ReadString());
            Assert.Equal("A", buffer.ReadString());
        }

        [Fact]
        public void String_UTF8()
        {
            using var buffer = new BitBuffer();

            buffer.AddStringUTF8("Olá Mundo!");
            Assert.Equal("Olá Mundo!", buffer.ReadStringUTF8());
        }

        // ═══════════════════════════════════════════════════════
        //  COMPRESSED FLOAT
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void CompressedFloat_RoundTrip()
        {
            using var buffer = new BitBuffer();

            // Position X: -500 to 500, precision 0.01
            buffer.AddCompressedFloat(-500f, 500f, 0.01f, 123.45f);

            float result = buffer.ReadCompressedFloat(-500f, 500f, 0.01f);

            Assert.InRange(result, 123.44f, 123.46f); // Allow small error
        }

        // ═══════════════════════════════════════════════════════
        //  BYTE ARRAY CONVERSION
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void ToArray_FromArray_RoundTrip()
        {
            using var writeBuffer = new BitBuffer();
            writeBuffer.AddBool(true);
            writeBuffer.AddUInt(10, 999);
            writeBuffer.AddFloat(3.14f);
            writeBuffer.AddString("Test");

            byte[] bytes = writeBuffer.ToArray();

            using var readBuffer = new BitBuffer();
            readBuffer.FromArray(bytes);

            Assert.True(readBuffer.ReadBool());
            Assert.Equal(999u, readBuffer.ReadUInt(10));
            Assert.Equal(3.14f, readBuffer.ReadFloat());
            Assert.Equal("Test", readBuffer.ReadString());
        }

        // ═══════════════════════════════════════════════════════
        //  FLUENT API
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void FluentAPI_Chaining()
        {
            using var buffer = new BitBuffer();

            buffer.AddBool(true)
                  .AddUInt(10, 500)
                  .AddFloat(1.5f)
                  .AddString("chain");

            Assert.True(buffer.ReadBool());
            Assert.Equal(500u, buffer.ReadUInt(10));
            Assert.Equal(1.5f, buffer.ReadFloat());
            Assert.Equal("chain", buffer.ReadString());
        }

        // ═══════════════════════════════════════════════════════
        //  BUFFER GROWTH
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void BufferGrowth_LargeData()
        {
            var buffer = new BitBuffer(1); // Start very small

            for (int i = 0; i < 1000; i++)
            {
                buffer.AddUInt((uint)i);
            }

            for (int i = 0; i < 1000; i++)
            {
                Assert.Equal((uint)i, buffer.ReadUInt());
            }
        }

        // ═══════════════════════════════════════════════════════
        //  EXTENSIONS
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void HalfPrecision_Extension()
        {
            using var buffer = new BitBuffer();

            buffer.AddHalf(1.5f);
            buffer.AddHalf(-42.0f);
            buffer.AddHalf(0f);

            Assert.Equal(1.5f, buffer.ReadHalf(), 0.01f);
            Assert.Equal(-42.0f, buffer.ReadHalf(), 0.5f);
            Assert.Equal(0f, buffer.ReadHalf(), 0.001f);
        }

        [Fact]
        public void BoundedRange_Vector3_Extension()
        {
            var range = new BoundedRange(-500f, 500f, 0.1f);
            using var buffer = new BitBuffer();

            buffer.AddVector3(range, 123.4f, -200.5f, 88.8f);
            buffer.ReadVector3(range, out float x, out float y, out float z);

            Assert.InRange(x, 123.3f, 123.5f);
            Assert.InRange(y, -200.6f, -200.4f);
            Assert.InRange(z, 88.7f, 88.9f);
        }

        [Fact]
        public void SmallestThree_Quaternion_Extension()
        {
            using var buffer = new BitBuffer();

            // Normalized quaternion
            float len = MathF.Sqrt(0.1f * 0.1f + 0.2f * 0.2f + 0.3f * 0.3f + 0.927f * 0.927f);
            float qx = 0.1f / len, qy = 0.2f / len, qz = 0.3f / len, qw = 0.927f / len;

            buffer.AddQuaternion(qx, qy, qz, qw);
            buffer.ReadQuaternion(out float rx, out float ry, out float rz, out float rw);

            // SmallestThree with 9 bits per component allows ~0.002 error
            Assert.InRange(rx, qx - 0.01f, qx + 0.01f);
            Assert.InRange(ry, qy - 0.01f, qy + 0.01f);
            Assert.InRange(rz, qz - 0.01f, qz + 0.01f);
            Assert.InRange(rw, qw - 0.01f, qw + 0.01f);
        }

        [Fact]
        public void Angle_NormalizedValue_Extensions()
        {
            using var buffer = new BitBuffer();

            buffer.AddAngle(10, 180f);
            buffer.AddNormalized(8, 0.75f);

            float angle = buffer.ReadAngle(10);
            float normalized = buffer.ReadNormalized(8);

            Assert.InRange(angle, 179f, 181f);
            Assert.InRange(normalized, 0.74f, 0.76f);
        }
    }
}
