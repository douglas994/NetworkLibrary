using System;
using Xunit;
using NetworkLibrary.Quantization;

namespace NetworkLibrary.Tests
{
    public class QuantizationTests
    {
        // ═══════════════════════════════════════════════════════
        //  HALF PRECISION
        // ═══════════════════════════════════════════════════════

        [Theory]
        [InlineData(0f)]
        [InlineData(1f)]
        [InlineData(-1f)]
        [InlineData(0.5f)]
        [InlineData(100f)]
        [InlineData(-100f)]
        [InlineData(65504f)]  // Max half value
        [InlineData(-65504f)] // Min half value
        public void HalfPrecision_RoundTrip(float value)
        {
            ushort quantized = HalfPrecision.Quantize(value);
            float dequantized = HalfPrecision.Dequantize(quantized);

            // Half precision has ~0.1% relative error
            if (value == 0f)
            {
                Assert.Equal(0f, dequantized);
            }
            else
            {
                float error = Math.Abs(dequantized - value) / Math.Abs(value);
                Assert.True(error < 0.01f, $"Value {value}: got {dequantized}, error {error:P}");
            }
        }

        [Fact]
        public void HalfPrecision_SpecialValues()
        {
            // Infinity
            ushort inf = HalfPrecision.Quantize(float.PositiveInfinity);
            Assert.True(float.IsPositiveInfinity(HalfPrecision.Dequantize(inf)));

            // Negative infinity
            ushort negInf = HalfPrecision.Quantize(float.NegativeInfinity);
            Assert.True(float.IsNegativeInfinity(HalfPrecision.Dequantize(negInf)));

            // Zero
            Assert.Equal(0f, HalfPrecision.Dequantize(HalfPrecision.Quantize(0f)));
        }

        // ═══════════════════════════════════════════════════════
        //  BOUNDED RANGE
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void BoundedRange_BasicRoundTrip()
        {
            var range = new BoundedRange(-500f, 500f, 0.01f);

            float[] testValues = { 0f, 100f, -100f, 499.99f, -500f, 250.5f };

            foreach (float value in testValues)
            {
                uint quantized = range.Quantize(value);
                float dequantized = range.Dequantize(quantized);

                float error = Math.Abs(dequantized - value);
                Assert.True(error < 0.02f, $"Value {value}: got {dequantized}, error {error}");
            }
        }

        [Fact]
        public void BoundedRange_Clamping()
        {
            var range = new BoundedRange(-100f, 100f, 0.1f);

            // Values outside range should be clamped
            uint overMax = range.Quantize(999f);
            float result = range.Dequantize(overMax);
            Assert.InRange(result, 99f, 101f);

            uint underMin = range.Quantize(-999f);
            result = range.Dequantize(underMin);
            Assert.InRange(result, -101f, -99f);
        }

        [Fact]
        public void BoundedRange_BitsRequired()
        {
            // Range 0-1000 with precision 1 → needs 10 bits (2^10 = 1024)
            var range = new BoundedRange(0f, 1000f, 1f);
            Assert.Equal(10, range.BitsRequired);

            // Range -500 to 500 with precision 0.01 → 100000 values → 17 bits
            var range2 = new BoundedRange(-500f, 500f, 0.01f);
            Assert.Equal(17, range2.BitsRequired);
        }

        [Fact]
        public void BoundedRange_Vector3()
        {
            var range = new BoundedRange(-1000f, 1000f, 0.1f);

            var q = range.QuantizeVector3(150.5f, -300.2f, 99.9f);
            range.DequantizeVector3(q, out float x, out float y, out float z);

            Assert.InRange(x, 150.4f, 150.6f);
            Assert.InRange(y, -300.3f, -300.1f);
            Assert.InRange(z, 99.8f, 100.0f);
        }

        [Fact]
        public void BoundedRange_StaticHelper()
        {
            float value = 42.5f;
            uint quantized = BoundedRangeHelper.Quantize(value, 0f, 100f, 0.1f);
            float dequantized = BoundedRangeHelper.Dequantize(quantized, 0f, 100f, 0.1f);

            Assert.InRange(dequantized, 42.4f, 42.6f);
        }

        // ═══════════════════════════════════════════════════════
        //  SMALLEST THREE (Quaternion)
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void SmallestThree_Identity()
        {
            // Identity quaternion (0, 0, 0, 1)
            var q = SmallestThree.Quantize(0f, 0f, 0f, 1f);
            SmallestThree.Dequantize(q, out float x, out float y, out float z, out float w);

            Assert.InRange(x, -0.01f, 0.01f);
            Assert.InRange(y, -0.01f, 0.01f);
            Assert.InRange(z, -0.01f, 0.01f);
            Assert.InRange(w, 0.99f, 1.01f);
        }

        [Fact]
        public void SmallestThree_90DegreeRotation()
        {
            // 90° around Y axis: (0, sin(45°), 0, cos(45°)) = (0, 0.707, 0, 0.707)
            float s = MathF.Sin(MathF.PI / 4);
            float c = MathF.Cos(MathF.PI / 4);

            var q = SmallestThree.Quantize(0f, s, 0f, c);
            SmallestThree.Dequantize(q, out float x, out float y, out float z, out float w);

            Assert.InRange(x, -0.01f, 0.01f);
            Assert.InRange(y, s - 0.01f, s + 0.01f);
            Assert.InRange(z, -0.01f, 0.01f);
            Assert.InRange(w, c - 0.01f, c + 0.01f);
        }

        [Fact]
        public void SmallestThree_ArbitraryRotation()
        {
            // Normalized arbitrary quaternion
            float qx = 0.1826f, qy = 0.3651f, qz = 0.5477f, qw = 0.7303f;
            float len = MathF.Sqrt(qx * qx + qy * qy + qz * qz + qw * qw);
            qx /= len; qy /= len; qz /= len; qw /= len;

            var q = SmallestThree.Quantize(qx, qy, qz, qw);
            SmallestThree.Dequantize(q, out float rx, out float ry, out float rz, out float rw);

            Assert.InRange(rx, qx - 0.01f, qx + 0.01f);
            Assert.InRange(ry, qy - 0.01f, qy + 0.01f);
            Assert.InRange(rz, qz - 0.01f, qz + 0.01f);
            Assert.InRange(rw, qw - 0.01f, qw + 0.01f);
        }

        [Fact]
        public void SmallestThree_TotalBits()
        {
            Assert.Equal(29, SmallestThree.TotalBits(9));  // Default
            Assert.Equal(32, SmallestThree.TotalBits(10)); // Higher precision
            Assert.Equal(26, SmallestThree.TotalBits(8));  // Lower precision
        }

        [Fact]
        public void SmallestThree_NegatedQuaternion()
        {
            // q and -q should produce similar results since they represent the same rotation
            float qx = 0.5f, qy = 0.5f, qz = 0.5f, qw = 0.5f;

            var q1 = SmallestThree.Quantize(qx, qy, qz, qw);
            var q2 = SmallestThree.Quantize(-qx, -qy, -qz, -qw);

            SmallestThree.Dequantize(q1, out float rx1, out float ry1, out float rz1, out float rw1);
            SmallestThree.Dequantize(q2, out float rx2, out float ry2, out float rz2, out float rw2);

            // They should be equivalent (same or negated)
            float dot = rx1 * rx2 + ry1 * ry2 + rz1 * rz2 + rw1 * rw2;
            Assert.True(Math.Abs(Math.Abs(dot) - 1f) < 0.02f);
        }
    }
}
