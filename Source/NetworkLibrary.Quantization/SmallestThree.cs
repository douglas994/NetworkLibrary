/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Quantization - SmallestThree
 *
 *  Compresses a quaternion (4 floats = 128 bits) to ~29 bits.
 *  Algorithm: Stores the index of the largest component (2 bits)
 *  and quantizes the remaining 3 smallest components (9 bits each).
 *  Based on Glenn Fiedler's snapshot compression techniques.
 */

using System;
using System.Runtime.CompilerServices;

namespace NetworkLibrary.Quantization
{
    /// <summary>
    /// Quantized quaternion representation using the Smallest Three algorithm.
    /// </summary>
    public struct QuantizedQuaternion
    {
        /// <summary>Index of the largest component (0-3), stored in 2 bits.</summary>
        public uint M;
        /// <summary>First smallest component, quantized.</summary>
        public uint A;
        /// <summary>Second smallest component, quantized.</summary>
        public uint B;
        /// <summary>Third smallest component, quantized.</summary>
        public uint C;

        public QuantizedQuaternion(uint m, uint a, uint b, uint c)
        {
            M = m;
            A = a;
            B = b;
            C = c;
        }
    }

    /// <summary>
    /// Compresses quaternion rotations from 128 bits to ~29 bits using the Smallest Three algorithm.
    /// The largest component is inferred from the other three (since |q| = 1).
    /// Typical savings: 77% bandwidth reduction for rotation data.
    /// </summary>
    public static class SmallestThree
    {
        // The maximum value of the smallest three components:
        // For a unit quaternion, if the largest component is w, the other 3 satisfy:
        // x² + y² + z² = 1 - w², so each is at most 1/√2 ≈ 0.7071
        private const float SmallestThreeMax = 0.7071068f; // 1/√2
        private const float SmallestThreeScale = 1.0f / SmallestThreeMax;

        /// <summary>
        /// Quantizes a unit quaternion to a QuantizedQuaternion.
        /// </summary>
        /// <param name="x">Quaternion X component.</param>
        /// <param name="y">Quaternion Y component.</param>
        /// <param name="z">Quaternion Z component.</param>
        /// <param name="w">Quaternion W component.</param>
        /// <param name="bitsPerComponent">Bits per smallest component (default 9 = 29 total bits).</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static QuantizedQuaternion Quantize(float x, float y, float z, float w, int bitsPerComponent = 9)
        {
            float absX = Math.Abs(x);
            float absY = Math.Abs(y);
            float absZ = Math.Abs(z);
            float absW = Math.Abs(w);

            // Find the index of the largest component
            uint maxIndex = 0;
            float maxValue = absX;

            if (absY > maxValue) { maxIndex = 1; maxValue = absY; }
            if (absZ > maxValue) { maxIndex = 2; maxValue = absZ; }
            if (absW > maxValue) { maxIndex = 3; }

            // Ensure the largest component is positive (negate quaternion if needed,
            // since q and -q represent the same rotation)
            float sign;

            switch (maxIndex)
            {
                case 0: sign = x < 0 ? -1f : 1f; break;
                case 1: sign = y < 0 ? -1f : 1f; break;
                case 2: sign = z < 0 ? -1f : 1f; break;
                default: sign = w < 0 ? -1f : 1f; break;
            }

            x *= sign;
            y *= sign;
            z *= sign;
            w *= sign;

            // Extract the three smallest components
            float a, b, c;

            switch (maxIndex)
            {
                case 0: a = y; b = z; c = w; break;
                case 1: a = x; b = z; c = w; break;
                case 2: a = x; b = y; c = w; break;
                default: a = x; b = y; c = z; break;
            }

            uint maxVal = (1u << bitsPerComponent) - 1;
            float halfMax = maxVal * 0.5f;

            // Normalize from [-1/√2, 1/√2] to [0, maxVal]
            uint qa = (uint)(((a * SmallestThreeScale) + 1f) * halfMax + 0.5f);
            uint qb = (uint)(((b * SmallestThreeScale) + 1f) * halfMax + 0.5f);
            uint qc = (uint)(((c * SmallestThreeScale) + 1f) * halfMax + 0.5f);

            // Clamp to valid range
            if (qa > maxVal) qa = maxVal;
            if (qb > maxVal) qb = maxVal;
            if (qc > maxVal) qc = maxVal;

            return new QuantizedQuaternion(maxIndex, qa, qb, qc);
        }

        /// <summary>
        /// Dequantizes a QuantizedQuaternion back to quaternion components.
        /// </summary>
        /// <param name="quantized">The quantized quaternion.</param>
        /// <param name="x">Output X component.</param>
        /// <param name="y">Output Y component.</param>
        /// <param name="z">Output Z component.</param>
        /// <param name="w">Output W component.</param>
        /// <param name="bitsPerComponent">Bits per component (must match quantization).</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Dequantize(QuantizedQuaternion quantized, out float x, out float y, out float z, out float w, int bitsPerComponent = 9)
        {
            uint maxVal = (1u << bitsPerComponent) - 1;
            float halfMax = maxVal * 0.5f;
            float invScale = SmallestThreeMax;

            // Denormalize from [0, maxVal] to [-1/√2, 1/√2]
            float a = ((quantized.A / halfMax) - 1f) * invScale;
            float b = ((quantized.B / halfMax) - 1f) * invScale;
            float c = ((quantized.C / halfMax) - 1f) * invScale;

            // Reconstruct the largest component
            float largest = MathF.Sqrt(1f - (a * a + b * b + c * c));

            switch (quantized.M)
            {
                case 0: x = largest; y = a; z = b; w = c; break;
                case 1: x = a; y = largest; z = b; w = c; break;
                case 2: x = a; y = b; z = largest; w = c; break;
                default: x = a; y = b; z = c; w = largest; break;
            }
        }

        /// <summary>
        /// Returns the total number of bits for a quantized quaternion.
        /// 2 bits for the index + bitsPerComponent × 3 for the smallest three.
        /// Default: 2 + 9×3 = 29 bits.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TotalBits(int bitsPerComponent = 9)
        {
            return 2 + (bitsPerComponent * 3);
        }
    }
}
