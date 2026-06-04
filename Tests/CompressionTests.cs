using System;
using Xunit;
using NetworkLibrary.Compression;

namespace NetworkLibrary.Tests
{
    public class CompressionTests
    {
        // ═══════════════════════════════════════════════════════
        //  DELTA ENCODER
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void DeltaEncoder_Basic()
        {
            uint delta = DeltaEncoder.EncodeDelta(1000, 1002);
            uint current = DeltaEncoder.DecodeDelta(1000, delta);

            Assert.Equal(2u, delta);
            Assert.Equal(1002u, current);
        }

        [Fact]
        public void DeltaEncoder_Signed()
        {
            // Moving backward (negative delta)
            uint encoded = DeltaEncoder.EncodeDeltaSigned(100, 95);
            int decoded = DeltaEncoder.DecodeDeltaSigned(100, encoded);

            Assert.Equal(95, decoded);
        }

        [Fact]
        public void DeltaEncoder_ZeroChange()
        {
            uint delta = DeltaEncoder.EncodeDelta(500, 500);
            Assert.Equal(0u, delta);

            uint result = DeltaEncoder.DecodeDelta(500, delta);
            Assert.Equal(500u, result);
        }

        [Fact]
        public void DeltaEncoder_Array()
        {
            uint[] values = { 100, 102, 105, 110, 120 };
            uint[] original = (uint[])values.Clone();

            DeltaEncoder.EncodeArray(values, 0, values.Length);

            // First value unchanged, rest are deltas
            Assert.Equal(100u, values[0]);
            Assert.Equal(2u, values[1]);  // 102 - 100
            Assert.Equal(3u, values[2]);  // 105 - 102
            Assert.Equal(5u, values[3]);  // 110 - 105
            Assert.Equal(10u, values[4]); // 120 - 110

            DeltaEncoder.DecodeArray(values, 0, values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                Assert.Equal(original[i], values[i]);
            }
        }

        // ═══════════════════════════════════════════════════════
        //  RUN-LENGTH ENCODER
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void RLE_BasicCompression()
        {
            byte[] input = { 0, 0, 0, 0, 0, 1, 1, 2 };
            byte[] output = new byte[64];

            int encoded = RunLengthEncoder.Encode(input, input.Length, output);

            // Expected: [5, 0] [2, 1] [1, 2]
            Assert.Equal(6, encoded);
            Assert.Equal(5, output[0]); // 5 zeros
            Assert.Equal(0, output[1]);
            Assert.Equal(2, output[2]); // 2 ones
            Assert.Equal(1, output[3]);
            Assert.Equal(1, output[4]); // 1 two
            Assert.Equal(2, output[5]);
        }

        [Fact]
        public void RLE_RoundTrip()
        {
            byte[] input = { 0, 0, 0, 0, 0, 255, 255, 1, 1, 1, 42 };
            byte[] encoded = new byte[64];
            byte[] decoded = new byte[64];

            int encLen = RunLengthEncoder.Encode(input, input.Length, encoded);
            int decLen = RunLengthEncoder.Decode(encoded, encLen, decoded);

            Assert.Equal(input.Length, decLen);

            for (int i = 0; i < input.Length; i++)
            {
                Assert.Equal(input[i], decoded[i]);
            }
        }

        [Fact]
        public void RLE_AllSameBytes()
        {
            byte[] input = new byte[200]; // All zeros
            byte[] output = new byte[64];

            int encoded = RunLengthEncoder.Encode(input, input.Length, output);

            // 200 zeros: [200, 0]
            Assert.Equal(2, encoded);
            Assert.Equal(200, output[0]);
            Assert.Equal(0, output[1]);
        }

        [Fact]
        public void RLE_AllDifferentBytes()
        {
            byte[] input = { 1, 2, 3, 4, 5 };
            byte[] encoded = new byte[64];
            byte[] decoded = new byte[64];

            int encLen = RunLengthEncoder.Encode(input, input.Length, encoded);
            int decLen = RunLengthEncoder.Decode(encoded, encLen, decoded);

            // Worst case: each byte = 2 bytes encoded
            Assert.Equal(10, encLen);
            Assert.Equal(input.Length, decLen);

            for (int i = 0; i < input.Length; i++)
            {
                Assert.Equal(input[i], decoded[i]);
            }
        }

        [Fact]
        public void RLE_EmptyInput()
        {
            byte[] input = Array.Empty<byte>();
            byte[] output = new byte[64];

            int encoded = RunLengthEncoder.Encode(input, 0, output);
            Assert.Equal(0, encoded);
        }

        [Fact]
        public void RLE_LongRunsCapped()
        {
            // Run of 300 zeros should be split (max run = 255)
            byte[] input = new byte[300];
            byte[] output = new byte[64];

            int encoded = RunLengthEncoder.Encode(input, input.Length, output);

            // Expected: [255, 0] [45, 0]
            Assert.Equal(4, encoded);
            Assert.Equal(255, output[0]);
            Assert.Equal(0, output[1]);
            Assert.Equal(45, output[2]);
            Assert.Equal(0, output[3]);
        }
    }
}
