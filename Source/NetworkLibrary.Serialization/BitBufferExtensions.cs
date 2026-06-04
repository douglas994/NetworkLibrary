/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Serialization - BitBufferExtensions
 *
 *  Extension methods for common MMORPG data types:
 *  Vector3 (with BoundedRange quantization), Quaternion (with SmallestThree),
 *  EntityId, HalfPrecision floats, etc.
 */

using System;
using System.Runtime.CompilerServices;
using NetworkLibrary.Quantization;

namespace NetworkLibrary.Serialization
{
    /// <summary>
    /// Extension methods for BitBuffer to handle common game data types.
    /// </summary>
    public static class BitBufferExtensions
    {
        // ═══════════════════════════════════════════════════════
        //  HALF PRECISION FLOAT (16 bits instead of 32)
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a float as half-precision (16 bits). Saves 50% bandwidth.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddHalf(this BitBuffer buffer, float value)
        {
            return buffer.AddRaw(16, HalfPrecision.Quantize(value));
        }

        /// <summary>
        /// Reads a half-precision float (16 bits).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadHalf(this BitBuffer buffer)
        {
            return HalfPrecision.Dequantize((ushort)buffer.ReadRaw(16));
        }

        // ═══════════════════════════════════════════════════════
        //  BOUNDED RANGE VECTORS (position compression)
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a 3D position using BoundedRange quantization.
        /// Example: AddVector3(range, 150.5f, 20.3f, -88.7f)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddVector3(this BitBuffer buffer, BoundedRange range, float x, float y, float z)
        {
            int bits = range.BitsRequired;
            buffer.AddRaw(bits, range.Quantize(x));
            buffer.AddRaw(bits, range.Quantize(y));
            buffer.AddRaw(bits, range.Quantize(z));
            return buffer;
        }

        /// <summary>
        /// Reads a 3D position using BoundedRange dequantization.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReadVector3(this BitBuffer buffer, BoundedRange range, out float x, out float y, out float z)
        {
            int bits = range.BitsRequired;
            x = range.Dequantize(buffer.ReadRaw(bits));
            y = range.Dequantize(buffer.ReadRaw(bits));
            z = range.Dequantize(buffer.ReadRaw(bits));
        }

        /// <summary>
        /// Writes a 2D position using BoundedRange quantization.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddVector2(this BitBuffer buffer, BoundedRange range, float x, float y)
        {
            int bits = range.BitsRequired;
            buffer.AddRaw(bits, range.Quantize(x));
            buffer.AddRaw(bits, range.Quantize(y));
            return buffer;
        }

        /// <summary>
        /// Reads a 2D position using BoundedRange dequantization.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReadVector2(this BitBuffer buffer, BoundedRange range, out float x, out float y)
        {
            int bits = range.BitsRequired;
            x = range.Dequantize(buffer.ReadRaw(bits));
            y = range.Dequantize(buffer.ReadRaw(bits));
        }

        // ═══════════════════════════════════════════════════════
        //  QUATERNION (SmallestThree compression)
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a quaternion using SmallestThree compression (128 → 29 bits by default).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddQuaternion(this BitBuffer buffer, float x, float y, float z, float w, int bitsPerComponent = 9)
        {
            QuantizedQuaternion q = SmallestThree.Quantize(x, y, z, w, bitsPerComponent);
            buffer.AddRaw(2, q.M);
            buffer.AddRaw(bitsPerComponent, q.A);
            buffer.AddRaw(bitsPerComponent, q.B);
            buffer.AddRaw(bitsPerComponent, q.C);
            return buffer;
        }

        /// <summary>
        /// Reads a quaternion using SmallestThree decompression.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReadQuaternion(this BitBuffer buffer, out float x, out float y, out float z, out float w, int bitsPerComponent = 9)
        {
            QuantizedQuaternion q = new QuantizedQuaternion(
                buffer.ReadRaw(2),
                buffer.ReadRaw(bitsPerComponent),
                buffer.ReadRaw(bitsPerComponent),
                buffer.ReadRaw(bitsPerComponent)
            );

            SmallestThree.Dequantize(q, out x, out y, out z, out w, bitsPerComponent);
        }

        // ═══════════════════════════════════════════════════════
        //  ENTITY ID (common MMORPG pattern)
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes an entity ID using variable-length encoding.
        /// Small IDs (0-127) use only 8 bits.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddEntityId(this BitBuffer buffer, uint entityId)
        {
            return buffer.AddUIntVar(entityId);
        }

        /// <summary>
        /// Reads an entity ID.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ReadEntityId(this BitBuffer buffer)
        {
            return buffer.ReadUIntVar();
        }

        // ═══════════════════════════════════════════════════════
        //  GAME-SPECIFIC HELPERS
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a normalized value (0.0 to 1.0) using the specified number of bits.
        /// Example: health percentage using 8 bits = 256 levels of precision.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddNormalized(this BitBuffer buffer, int numBits, float value)
        {
            uint maxVal = (1u << numBits) - 1;
            float clamped = Math.Clamp(value, 0f, 1f);
            uint quantized = (uint)(clamped * maxVal + 0.5f);
            return buffer.AddRaw(numBits, quantized);
        }

        /// <summary>
        /// Reads a normalized value (0.0 to 1.0).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadNormalized(this BitBuffer buffer, int numBits)
        {
            uint maxVal = (1u << numBits) - 1;
            uint quantized = buffer.ReadRaw(numBits);
            return (float)quantized / maxVal;
        }

        /// <summary>
        /// Writes an angle in degrees (0-360) using the specified number of bits.
        /// 10 bits = 0.35° precision, 8 bits = 1.4° precision.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitBuffer AddAngle(this BitBuffer buffer, int numBits, float degrees)
        {
            uint maxVal = (1u << numBits) - 1;
            float normalized = ((degrees % 360f) + 360f) % 360f / 360f;
            uint quantized = (uint)(normalized * maxVal + 0.5f);
            return buffer.AddRaw(numBits, quantized);
        }

        /// <summary>
        /// Reads an angle in degrees (0-360).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadAngle(this BitBuffer buffer, int numBits)
        {
            uint maxVal = (1u << numBits) - 1;
            uint quantized = buffer.ReadRaw(numBits);
            return (float)quantized / maxVal * 360f;
        }
    }
}
