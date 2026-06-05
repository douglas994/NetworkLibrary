/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Serialization
 *
 *  Compact bit-packing serializer. Writes individual bits instead of full bytes,
 *  enabling massive bandwidth savings for game state data.
 *
 *  Features:
 *  - Bit-level packing (e.g., HP 0-1000 in 10 bits instead of 32)
 *  - ZigZag encoding for signed integers
 *  - Variable-length encoding for small values
 *  - Fluent API for chaining writes
 *  - Zero allocations on hot paths
 */

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace NetworkLibrary.Serialization
{
    /// <summary>
    /// High-performance bit-level serialization buffer.
    /// Writes and reads data at arbitrary bit widths for maximum bandwidth efficiency.
    /// </summary>
    public struct BitBuffer : IDisposable
    {
        private const int DefaultCapacity = 375; // 375 * 4 = 1500 bytes (standard MTU)
        private const int StringLengthBits = 8;
        private const int StringLengthMax = (1 << StringLengthBits) - 1; // 255
        private const int BitsASCII = 7;
        private const int GrowFactor = 2;
        private const int MinGrow = 1;

        private uint[] _chunks;
        private int _readPosition;
        private int _writePosition;

        /// <summary>
        /// Creates a new BitBuffer with the specified capacity in uint chunks.
        /// Default capacity = 1500 bytes (standard MTU).
        /// </summary>
        public BitBuffer(int capacity = DefaultCapacity)
        {
            _chunks = ArrayPool<uint>.Shared.Rent(capacity);
            Array.Clear(_chunks, 0, _chunks.Length); // Zeroed to prevent dirty-pool bugs
            _readPosition = 0;
            _writePosition = 0;
        }

        /// <summary>
        /// Disposes the BitBuffer, returning its internal array to the pool.
        /// Call this using 'using var buffer = new BitBuffer();' or explicitly.
        /// </summary>
        public void Dispose()
        {
            if (_chunks != null)
            {
                ArrayPool<uint>.Shared.Return(_chunks);
                _chunks = null!;
            }
        }

        /// <summary>
        /// Length in bytes of the data currently written.
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (_writePosition + 7) >> 3;
        }

        /// <summary>
        /// Length in bits of the data currently written.
        /// </summary>
        public int BitsWritten
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _writePosition;
        }

        /// <summary>
        /// Length in bits remaining to be read.
        /// </summary>
        public int BitsAvailable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _writePosition - _readPosition;
        }

        /// <summary>
        /// Whether all written data has been read.
        /// </summary>
        public bool IsFinished
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _readPosition >= _writePosition;
        }

        /// <summary>
        /// Resets the buffer for reuse without deallocating memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            // Zero out the chunks that were written so reuse doesn't see stale bits
            if (_chunks != null && _writePosition > 0)
            {
                int usedChunks = (_writePosition + 31) >> 5;
                Array.Clear(_chunks, 0, usedChunks);
            }
            _readPosition = 0;
            _writePosition = 0;
        }

        // ═══════════════════════════════════════════════════════
        //  RAW BIT OPERATIONS
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a value using exactly the specified number of bits (1-32).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddRaw(int numBits, uint value)
        {
            Debug.Assert(numBits > 0 && numBits <= 32, "numBits must be between 1 and 32");

            if (numBits < 32)
                value &= (1u << numBits) - 1;

            EnsureCapacity(_writePosition + numBits);

            int chunkIndex = _writePosition >> 5;
            int bitOffset = _writePosition & 31;

            _chunks[chunkIndex] |= value << bitOffset;

            if (bitOffset + numBits > 32)
            {
                _chunks[chunkIndex + 1] = value >> (32 - bitOffset);
            }

            _writePosition += numBits;
            return this;
        }

        /// <summary>
        /// Reads a value using exactly the specified number of bits (1-32).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadRaw(int numBits)
        {
            Debug.Assert(numBits > 0 && numBits <= 32, "numBits must be between 1 and 32");
            Debug.Assert(_readPosition + numBits <= _writePosition, "Read past end of buffer");

            int chunkIndex = _readPosition >> 5;
            int bitOffset = _readPosition & 31;

            uint value = _chunks[chunkIndex] >> bitOffset;

            if (bitOffset + numBits > 32)
            {
                value |= _chunks[chunkIndex + 1] << (32 - bitOffset);
            }

            if (numBits < 32)
                value &= (1u << numBits) - 1;

            _readPosition += numBits;
            return value;
        }

        /// <summary>
        /// Peeks at a value without advancing the read position.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint PeekRaw(int numBits)
        {
            uint value = ReadRaw(numBits);
            _readPosition -= numBits;
            return value;
        }

        // ═══════════════════════════════════════════════════════
        //  BOOLEAN
        // ═══════════════════════════════════════════════════════

        /// <summary>Writes a boolean as 1 bit.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddBool(bool value)
        {
            return AddRaw(1, value ? 1u : 0u);
        }

        /// <summary>Reads a boolean from 1 bit.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ReadBool()
        {
            return ReadRaw(1) == 1;
        }

        // ═══════════════════════════════════════════════════════
        //  UNSIGNED INTEGERS
        // ═══════════════════════════════════════════════════════

        /// <summary>Writes a byte (8 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddByte(byte value)
        {
            return AddRaw(8, value);
        }

        /// <summary>Reads a byte (8 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte()
        {
            return (byte)ReadRaw(8);
        }

        /// <summary>Writes an unsigned short (16 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddUShort(ushort value)
        {
            return AddRaw(16, value);
        }

        /// <summary>Reads an unsigned short (16 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUShort()
        {
            return (ushort)ReadRaw(16);
        }

        /// <summary>Writes an unsigned int using the specified number of bits.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddUInt(int numBits, uint value)
        {
            return AddRaw(numBits, value);
        }

        /// <summary>Writes an unsigned int (full 32 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddUInt(uint value)
        {
            return AddRaw(32, value);
        }

        /// <summary>Reads an unsigned int using the specified number of bits.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt(int numBits)
        {
            return ReadRaw(numBits);
        }

        /// <summary>Reads an unsigned int (full 32 bits).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt()
        {
            return ReadRaw(32);
        }

        // ═══════════════════════════════════════════════════════
        //  SIGNED INTEGERS (ZigZag encoding)
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a signed integer using ZigZag encoding.
        /// Maps negatives to positives: 0→0, -1→1, 1→2, -2→3, 2→4, etc.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddInt(int numBits, int value)
        {
            uint zigzag = (uint)((value << 1) ^ (value >> 31));
            return AddRaw(numBits, zigzag);
        }

        /// <summary>Writes a signed int (full 32 bits with ZigZag).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddInt(int value)
        {
            return AddInt(32, value);
        }

        /// <summary>
        /// Reads a signed integer with ZigZag decoding.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt(int numBits)
        {
            uint zigzag = ReadRaw(numBits);
            return (int)((zigzag >> 1) ^ (uint)-(int)(zigzag & 1));
        }

        /// <summary>Reads a signed int (full 32 bits with ZigZag).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt()
        {
            return ReadInt(32);
        }

        // ═══════════════════════════════════════════════════════
        //  VARIABLE-LENGTH ENCODING
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes an unsigned int with variable-length encoding.
        /// Small values use fewer bits. Each group of 7 bits uses 1 continuation bit.
        /// Values 0-127 = 8 bits, 128-16383 = 16 bits, etc.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddUIntVar(uint value)
        {
            do
            {
                uint chunk = value & 0x7Fu;
                value >>= 7;

                if (value > 0)
                    chunk |= 0x80u; // continuation bit

                AddRaw(8, chunk);
            }
            while (value > 0);

            return this;
        }

        /// <summary>
        /// Reads an unsigned int with variable-length decoding.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUIntVar()
        {
            uint value = 0;
            int shift = 0;
            uint chunk;

            do
            {
                chunk = ReadRaw(8);
                value |= (chunk & 0x7Fu) << shift;
                shift += 7;
            }
            while ((chunk & 0x80u) != 0 && shift < 35);

            return value;
        }

        /// <summary>
        /// Writes a signed int with variable-length encoding (ZigZag + VarLength).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddIntVar(int value)
        {
            uint zigzag = (uint)((value << 1) ^ (value >> 31));
            return AddUIntVar(zigzag);
        }

        /// <summary>
        /// Reads a signed int with variable-length decoding (VarLength + ZigZag).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadIntVar()
        {
            uint zigzag = ReadUIntVar();
            return (int)((zigzag >> 1) ^ (uint)-(int)(zigzag & 1));
        }

        // ═══════════════════════════════════════════════════════
        //  FLOATING POINT
        // ═══════════════════════════════════════════════════════

        /// <summary>Writes a float (32 bits, IEEE 754).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddFloat(float value)
        {
            uint bits = FloatToUInt(value);
            return AddRaw(32, bits);
        }

        /// <summary>Reads a float (32 bits, IEEE 754).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadFloat()
        {
            uint bits = ReadRaw(32);
            return UIntToFloat(bits);
        }

        /// <summary>
        /// Writes a float compressed within a known range with specified precision.
        /// Example: AddCompressedFloat(-500f, 500f, 0.01f, position.x)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BitBuffer AddCompressedFloat(float min, float max, float precision, float value)
        {
            float range = max - min;
            float maxVal = range / precision;
            int numBits = BitsRequired((uint)maxVal);

            float clamped = Math.Clamp(value, min, max);
            float normalized = (clamped - min) / range;
            uint quantized = (uint)(normalized * maxVal + 0.5f);

            return AddRaw(numBits, quantized);
        }

        /// <summary>
        /// Reads a float compressed within a known range with specified precision.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadCompressedFloat(float min, float max, float precision)
        {
            float range = max - min;
            float maxVal = range / precision;
            int numBits = BitsRequired((uint)maxVal);

            uint quantized = ReadRaw(numBits);
            return quantized / maxVal * range + min;
        }

        // ═══════════════════════════════════════════════════════
        //  STRING
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes a string using 7-bit ASCII encoding.
        /// Max length: 255 characters.
        /// </summary>
        public BitBuffer AddString(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            int length = Math.Min(value.Length, StringLengthMax);
            AddRaw(StringLengthBits, (uint)length);

            for (int i = 0; i < length; i++)
            {
                AddRaw(BitsASCII, (uint)(value[i] & 0x7F));
            }

            return this;
        }

        /// <summary>
        /// Reads a string with 7-bit ASCII encoding.
        /// </summary>
        public string ReadString()
        {
            int length = (int)ReadRaw(StringLengthBits);
            // NOTE: Cannot use string.Create(length, this, ...) because BitBuffer is a struct.
            // Passing 'this' by value to the lambda means _readPosition updates inside the
            // lambda do NOT propagate back to the caller — causing 'Read past end of buffer'.
            var chars = new char[length];
            for (int i = 0; i < length; i++)
            {
                chars[i] = (char)ReadRaw(BitsASCII);
            }
            return new string(chars);
        }

        /// <summary>
        /// Writes a UTF-8 string. Supports full Unicode but uses more bandwidth.
        /// Max length: 255 bytes.
        /// </summary>
        public BitBuffer AddStringUTF8(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            int byteCount = Encoding.UTF8.GetByteCount(value);
            int length = Math.Min(byteCount, StringLengthMax);

            AddRaw(StringLengthBits, (uint)length);

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            Span<byte> utf8Bytes = length <= 256 ? stackalloc byte[length] : new byte[length];
            Encoding.UTF8.GetBytes(value.AsSpan(), utf8Bytes);

            for (int i = 0; i < length; i++)
            {
                AddRaw(8, utf8Bytes[i]);
            }
#else
            byte[] utf8Bytes = Encoding.UTF8.GetBytes(value);
            for (int i = 0; i < length; i++)
            {
                AddRaw(8, utf8Bytes[i]);
            }
#endif

            return this;
        }

        /// <summary>
        /// Reads a UTF-8 string.
        /// </summary>
        public string ReadStringUTF8()
        {
            int length = (int)ReadRaw(StringLengthBits);

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            Span<byte> utf8Bytes = length <= 256 ? stackalloc byte[length] : new byte[length];

            for (int i = 0; i < length; i++)
            {
                utf8Bytes[i] = (byte)ReadRaw(8);
            }

            return Encoding.UTF8.GetString(utf8Bytes);
#else
            byte[] utf8Bytes = new byte[length];
            for (int i = 0; i < length; i++)
            {
                utf8Bytes[i] = (byte)ReadRaw(8);
            }
            return Encoding.UTF8.GetString(utf8Bytes);
#endif
        }

        // ═══════════════════════════════════════════════════════
        //  BYTE ARRAY / SPAN CONVERSION
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Writes the buffer contents into a byte array.
        /// </summary>
        public int ToArray(byte[] destination)
        {
            int byteLength = Length;

            if (destination.Length < byteLength)
                throw new ArgumentException("Destination array is too small", nameof(destination));

            int chunkCount = (byteLength + 3) >> 2;

            for (int i = 0; i < chunkCount; i++)
            {
                uint chunk = _chunks[i];
                int offset = i << 2;
                int remaining = byteLength - offset;

                destination[offset] = (byte)chunk;
                if (remaining > 1) destination[offset + 1] = (byte)(chunk >> 8);
                if (remaining > 2) destination[offset + 2] = (byte)(chunk >> 16);
                if (remaining > 3) destination[offset + 3] = (byte)(chunk >> 24);
            }

            return byteLength;
        }

        /// <summary>
        /// Creates a new byte array with the buffer contents.
        /// </summary>
        public byte[] ToArray()
        {
            byte[] result = new byte[Length];
            ToArray(result);
            return result;
        }

        /// <summary>
        /// Loads data from a byte array into the buffer for reading.
        /// </summary>
        public void FromArray(byte[] source, int length)
        {
            int chunkCount = (length + 3) >> 2;

            if (_chunks == null || _chunks.Length < chunkCount)
            {
                if (_chunks != null) ArrayPool<uint>.Shared.Return(_chunks);
                _chunks = ArrayPool<uint>.Shared.Rent(chunkCount);
            }

            Buffer.BlockCopy(source, 0, _chunks, 0, length);
            Array.Clear(_chunks, chunkCount, _chunks.Length - chunkCount);

            _readPosition = 0;
            _writePosition = length * 8;
        }

        /// <summary>
        /// Loads data from a byte array into the buffer for reading.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FromArray(byte[] source)
        {
            FromArray(source, source.Length);
        }

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
        /// <summary>
        /// Writes the buffer contents into a Span (zero-copy friendly).
        /// </summary>
        public int ToSpan(Span<byte> destination)
        {
            int byteLength = Length;

            if (destination.Length < byteLength)
                throw new ArgumentException("Destination span is too small", nameof(destination));

            int chunkCount = (byteLength + 3) >> 2;

            for (int i = 0; i < chunkCount; i++)
            {
                uint chunk = _chunks[i];
                int offset = i << 2;
                int remaining = byteLength - offset;

                destination[offset] = (byte)chunk;
                if (remaining > 1) destination[offset + 1] = (byte)(chunk >> 8);
                if (remaining > 2) destination[offset + 2] = (byte)(chunk >> 16);
                if (remaining > 3) destination[offset + 3] = (byte)(chunk >> 24);
            }

            return byteLength;
        }

        /// <summary>
        /// Loads data from a ReadOnlySpan into the buffer for reading.
        /// </summary>
        public void FromSpan(ReadOnlySpan<byte> source)
        {
            int length = source.Length;
            int chunkCount = (length + 3) >> 2;

            if (_chunks == null || _chunks.Length < chunkCount)
            {
                if (_chunks != null) ArrayPool<uint>.Shared.Return(_chunks);
                _chunks = ArrayPool<uint>.Shared.Rent(chunkCount);
            }

            // Zero chunk array completely first (prevents dirty trailing bits)
            Array.Clear(_chunks, 0, Math.Min(chunkCount + 1, _chunks.Length));
            // Copy bytes one-by-one into the uint array as raw bytes
            source.CopyTo(MemoryMarshal.AsBytes(new Span<uint>(_chunks, 0, chunkCount)));

            // Zero out remainder of the rented array beyond what we used
            if (_chunks.Length > chunkCount)
                Array.Clear(_chunks, chunkCount, _chunks.Length - chunkCount);

            _readPosition = 0;
            _writePosition = length * 8;
        }
#endif

        // ═══════════════════════════════════════════════════════
        //  UTILITIES
        // ═══════════════════════════════════════════════════════

        /// <summary>
        /// Returns the number of bits needed to represent a value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BitsRequired(uint maxValue)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureCapacity(int totalBits)
        {
            int chunkIndex = (totalBits + 31) >> 5;
            if (_chunks == null)
            {
                _chunks = ArrayPool<uint>.Shared.Rent(DefaultCapacity);
                Array.Clear(_chunks, 0, _chunks.Length);
            }

            int currentCapacity = _chunks.Length;
            if (chunkIndex >= currentCapacity)
            {
                int newCapacity = Math.Max(currentCapacity * GrowFactor, currentCapacity + MinGrow);
                newCapacity = Math.Max(newCapacity, chunkIndex + 1);

                uint[] newChunks = ArrayPool<uint>.Shared.Rent(newCapacity);
                // Copy existing data, then zero out the rest to prevent dirty-pool corruption
                Array.Copy(_chunks, newChunks, currentCapacity);
                Array.Clear(newChunks, currentCapacity, newChunks.Length - currentCapacity);
                ArrayPool<uint>.Shared.Return(_chunks);
                _chunks = newChunks;
            }
        }

        // ═══════════════════════════════════════════════════════
        //  FLOAT ↔ UINT CONVERSION (union-style, zero alloc)
        // ═══════════════════════════════════════════════════════

        [StructLayout(LayoutKind.Explicit)]
        private struct FloatUIntUnion
        {
            [FieldOffset(0)] public float FloatValue;
            [FieldOffset(0)] public uint UIntValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint FloatToUInt(float value)
        {
            FloatUIntUnion union = default;
            union.FloatValue = value;
            return union.UIntValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static float UIntToFloat(uint value)
        {
            FloatUIntUnion union = default;
            union.UIntValue = value;
            return union.FloatValue;
        }
    }
}
