/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Quantization - BoundedRange
 *
 *  Compresses float values within a known range to N bits.
 *  Example: position.x from -500 to 500 with 0.01 precision → 17 bits instead of 32.
 *  Also provides Vector2/Vector3 quantization for positions.
 */

using System;
using System.Runtime.CompilerServices;

namespace NetworkLibrary.Quantization
{
    /// <summary>
    /// Quantized 2D vector with uint components.
    /// </summary>
    public struct QuantizedVector2
    {
        public uint X;
        public uint Y;

        public QuantizedVector2(uint x, uint y)
        {
            X = x;
            Y = y;
        }
    }

    /// <summary>
    /// Quantized 3D vector with uint components.
    /// </summary>
    public struct QuantizedVector3
    {
        public uint X;
        public uint Y;
        public uint Z;

        public QuantizedVector3(uint x, uint y, uint z)
        {
            X = x;
            Y = y;
            Z = z;
        }
    }

    /// <summary>
    /// Compresses float values within a known [min, max] range to a fixed number of bits.
    /// Ideal for game positions, speeds, and other bounded values.
    /// </summary>
    public sealed class BoundedRange
    {
        private readonly float _min;
        private readonly float _max;
        private readonly float _precision;
        private readonly int _bitsRequired;
        private readonly uint _maxIntegerValue;
        private readonly float _range;

        /// <summary>Number of bits used to represent the quantized value.</summary>
        public int BitsRequired => _bitsRequired;

        /// <summary>
        /// Creates a BoundedRange compressor for a specific range and precision.
        /// </summary>
        /// <param name="min">Minimum value in the range.</param>
        /// <param name="max">Maximum value in the range.</param>
        /// <param name="precision">The smallest representable difference (e.g., 0.01).</param>
        public BoundedRange(float min, float max, float precision)
        {
            _min = min;
            _max = max;
            _precision = precision;
            _range = max - min;
            _maxIntegerValue = (uint)(_range / precision + 0.5f);
            _bitsRequired = ComputeBitsRequired(_maxIntegerValue);
        }

        /// <summary>
        /// Quantizes a float value to a uint within the bounded range.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint Quantize(float value)
        {
            float clamped = Math.Clamp(value, _min, _max);
            float normalized = (clamped - _min) / _range;
            return (uint)(normalized * _maxIntegerValue + 0.5f);
        }

        /// <summary>
        /// Dequantizes a uint value back to a float within the bounded range.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float Dequantize(uint quantized)
        {
            float normalized = (float)quantized / _maxIntegerValue;
            return normalized * _range + _min;
        }

        /// <summary>
        /// Quantizes a 2D vector.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QuantizedVector2 QuantizeVector2(float x, float y)
        {
            return new QuantizedVector2(Quantize(x), Quantize(y));
        }

        /// <summary>
        /// Quantizes a 3D vector.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QuantizedVector3 QuantizeVector3(float x, float y, float z)
        {
            return new QuantizedVector3(Quantize(x), Quantize(y), Quantize(z));
        }

        /// <summary>
        /// Dequantizes a 2D vector, returning individual components.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DequantizeVector2(QuantizedVector2 quantized, out float x, out float y)
        {
            x = Dequantize(quantized.X);
            y = Dequantize(quantized.Y);
        }

        /// <summary>
        /// Dequantizes a 3D vector, returning individual components.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DequantizeVector3(QuantizedVector3 quantized, out float x, out float y, out float z)
        {
            x = Dequantize(quantized.X);
            y = Dequantize(quantized.Y);
            z = Dequantize(quantized.Z);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ComputeBitsRequired(uint maxValue)
        {
            if (maxValue == 0) return 1;

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            return 32 - int.LeadingZeroCount((int)maxValue);
#else
            int bits = 0;
            while (maxValue > 0)
            {
                bits++;
                maxValue >>= 1;
            }
            return bits;
#endif
        }
    }

    /// <summary>
    /// Static helper for one-off bounded range quantization without creating a BoundedRange instance.
    /// </summary>
    public static class BoundedRangeHelper
    {
        /// <summary>
        /// Quantizes a float value within a range.
        /// For repeated use with the same range, prefer creating a <see cref="BoundedRange"/> instance.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Quantize(float value, float min, float max, float precision)
        {
            float range = max - min;
            uint maxInteger = (uint)(range / precision + 0.5f);
            float clamped = Math.Clamp(value, min, max);
            float normalized = (clamped - min) / range;
            return (uint)(normalized * maxInteger + 0.5f);
        }

        /// <summary>
        /// Dequantizes a uint value back to float.
        /// For repeated use with the same range, prefer creating a <see cref="BoundedRange"/> instance.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float Dequantize(uint quantized, float min, float max, float precision)
        {
            float range = max - min;
            uint maxInteger = (uint)(range / precision + 0.5f);
            float normalized = (float)quantized / maxInteger;
            return normalized * range + min;
        }

        /// <summary>
        /// Returns the number of bits required to represent a value in the given range and precision.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BitsRequired(float min, float max, float precision)
        {
            float range = max - min;
            uint maxInteger = (uint)(range / precision + 0.5f);

            if (maxInteger == 0) return 1;

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            return 32 - int.LeadingZeroCount((int)maxInteger);
#else
            int bits = 0;
            while (maxInteger > 0)
            {
                bits++;
                maxInteger >>= 1;
            }
            return bits;
#endif
        }
    }
}
