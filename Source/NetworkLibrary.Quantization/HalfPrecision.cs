/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Quantization - HalfPrecision
 *
 *  Converts float (32-bit) to half-precision (16-bit) and back.
 *  Saves 50% bandwidth for values where full precision isn't needed.
 *  Uses IEEE 754 half-precision format via union-based conversion (zero alloc).
 */

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NetworkLibrary.Quantization
{
    /// <summary>
    /// Converts between 32-bit float and 16-bit half-precision float.
    /// Saves 50% bandwidth for values that don't require full float precision.
    /// </summary>
    public static class HalfPrecision
    {
        [StructLayout(LayoutKind.Explicit)]
        private struct FloatUInt
        {
            [FieldOffset(0)] public float FloatValue;
            [FieldOffset(0)] public uint UIntValue;
        }

        /// <summary>
        /// Quantizes a 32-bit float to 16-bit half-precision.
        /// Range: ±65504, smallest subnormal: ~5.96e-8
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort Quantize(float value)
        {
            FloatUInt u = default;
            u.FloatValue = value;
            uint f = u.UIntValue;

            // Extract components
            uint sign = (f >> 16) & 0x8000u;
            int exponent = (int)((f >> 23) & 0xFFu) - 127 + 15;
            uint mantissa = f & 0x007FFFFFu;

            // Handle special cases
            if (exponent <= 0)
            {
                if (exponent < -10)
                    return (ushort)sign; // Too small, flush to zero

                // Subnormal half
                mantissa |= 0x00800000u;
                int shift = 14 - exponent;
                uint round = (1u << (shift - 1)) - 1;
                mantissa += round;
                return (ushort)(sign | (mantissa >> shift));
            }

            if (exponent == 0xFF - 127 + 15)
            {
                if (mantissa == 0)
                    return (ushort)(sign | 0x7C00u); // Infinity

                // NaN
                mantissa >>= 13;
                return (ushort)(sign | 0x7C00u | mantissa | (mantissa == 0 ? 1u : 0u));
            }

            // Round
            mantissa += 0x00000FFFu + ((mantissa >> 13) & 1u);

            if ((mantissa & 0x00800000u) != 0)
            {
                mantissa = 0;
                exponent++;
            }

            if (exponent > 30)
                return (ushort)(sign | 0x7C00u); // Overflow to infinity

            return (ushort)(sign | ((uint)exponent << 10) | (mantissa >> 13));
        }

        /// <summary>
        /// Dequantizes a 16-bit half-precision value back to 32-bit float.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float Dequantize(ushort value)
        {
            uint sign = (uint)(value & 0x8000) << 16;
            uint exponent = (uint)(value >> 10) & 0x1Fu;
            uint mantissa = (uint)(value & 0x03FFu);

            uint result;

            if (exponent == 0)
            {
                if (mantissa == 0)
                {
                    result = sign; // Zero
                }
                else
                {
                    // Subnormal
                    exponent = 1;

                    while ((mantissa & 0x0400u) == 0)
                    {
                        mantissa <<= 1;
                        exponent--;
                    }

                    mantissa &= 0x03FFu;
                    result = sign | ((127 - 15 + exponent) << 23) | (mantissa << 13);
                }
            }
            else if (exponent == 31)
            {
                // Inf or NaN
                result = sign | 0x7F800000u | (mantissa << 13);
            }
            else
            {
                // Normalized
                result = sign | ((exponent + 127 - 15) << 23) | (mantissa << 13);
            }

            FloatUInt u = default;
            u.UIntValue = result;
            return u.FloatValue;
        }
    }
}
