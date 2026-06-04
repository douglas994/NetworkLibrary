/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Compression - DeltaEncoder
 *
 *  Delta encoding for game state snapshots. Sends only the difference between
 *  previous and current state, dramatically reducing bandwidth for slow-changing data.
 *  Example: Entity at position 1000, moves to 1002 → sends delta of 2 instead of 1002.
 */

using System;
using System.Runtime.CompilerServices;

namespace NetworkLibrary.Compression
{
    /// <summary>
    /// Delta encoding/decoding for network state synchronization.
    /// Encodes differences between sequential values for bandwidth savings.
    /// Combine with variable-length encoding for maximum compression.
    /// </summary>
    public static class DeltaEncoder
    {
        /// <summary>
        /// Encodes the delta between previous and current uint values.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint EncodeDelta(uint previous, uint current)
        {
            return current - previous;
        }

        /// <summary>
        /// Decodes a delta back to the current value given the previous value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint DecodeDelta(uint previous, uint delta)
        {
            return previous + delta;
        }

        /// <summary>
        /// Encodes the delta between previous and current int values using ZigZag.
        /// Handles positive and negative deltas efficiently.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint EncodeDeltaSigned(int previous, int current)
        {
            int delta = current - previous;
            // ZigZag encode: maps negative deltas to positive for better compression
            return (uint)((delta << 1) ^ (delta >> 31));
        }

        /// <summary>
        /// Decodes a ZigZag-encoded signed delta.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DecodeDeltaSigned(int previous, uint encodedDelta)
        {
            int delta = (int)((encodedDelta >> 1) ^ (uint)-(int)(encodedDelta & 1));
            return previous + delta;
        }

        /// <summary>
        /// Delta-encodes an array of uint values in-place.
        /// After encoding, values[i] contains the delta from values[i-1].
        /// </summary>
        public static void EncodeArray(uint[] values, int offset, int count)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));
            if (offset < 0 || count < 0 || offset + count > values.Length)
                throw new ArgumentOutOfRangeException();

            // Encode in reverse to avoid clobbering values we still need
            for (int i = offset + count - 1; i > offset; i--)
            {
                values[i] = values[i] - values[i - 1];
            }
        }

        /// <summary>
        /// Delta-decodes an array of uint values in-place.
        /// Restores original values from deltas.
        /// </summary>
        public static void DecodeArray(uint[] values, int offset, int count)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));
            if (offset < 0 || count < 0 || offset + count > values.Length)
                throw new ArgumentOutOfRangeException();

            for (int i = offset + 1; i < offset + count; i++)
            {
                values[i] = values[i] + values[i - 1];
            }
        }

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
        /// <summary>
        /// Delta-encodes a Span of uint values in-place.
        /// </summary>
        public static void EncodeSpan(Span<uint> values)
        {
            for (int i = values.Length - 1; i > 0; i--)
            {
                values[i] = values[i] - values[i - 1];
            }
        }

        /// <summary>
        /// Delta-decodes a Span of uint values in-place.
        /// </summary>
        public static void DecodeSpan(Span<uint> values)
        {
            for (int i = 1; i < values.Length; i++)
            {
                values[i] = values[i] + values[i - 1];
            }
        }
#endif
    }
}
