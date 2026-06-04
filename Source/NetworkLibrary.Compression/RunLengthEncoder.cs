/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Compression - RunLengthEncoder
 *
 *  Run-Length Encoding for sparse data (tile maps, entity flags, visibility masks).
 *  Encodes consecutive repeated bytes as (count, value) pairs.
 */

using System;
using System.Runtime.CompilerServices;

namespace NetworkLibrary.Compression
{
    /// <summary>
    /// Run-Length Encoding (RLE) for compressing sparse game data.
    /// Effective for data with many consecutive repeated values
    /// (e.g., tile maps, visibility masks, flag arrays).
    /// </summary>
    public static class RunLengthEncoder
    {
        /// <summary>
        /// Maximum run length per RLE pair (255, stored in 1 byte).
        /// </summary>
        private const int MaxRunLength = 255;

        /// <summary>
        /// Encodes data using Run-Length Encoding.
        /// Format: [count][value][count][value]...
        /// </summary>
        /// <param name="input">Source data to encode.</param>
        /// <param name="inputLength">Number of bytes to encode from input.</param>
        /// <param name="output">Destination buffer for encoded data.</param>
        /// <returns>Number of bytes written to output, or -1 if output is too small.</returns>
        public static int Encode(byte[] input, int inputLength, byte[] output)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));
            if (output == null) throw new ArgumentNullException(nameof(output));
            if (inputLength <= 0) return 0;

            int writePos = 0;

            int i = 0;
            while (i < inputLength)
            {
                byte current = input[i];
                int runLength = 1;

                while (i + runLength < inputLength && input[i + runLength] == current && runLength < MaxRunLength)
                {
                    runLength++;
                }

                // Need 2 bytes for each run (count + value)
                if (writePos + 2 > output.Length)
                    return -1;

                output[writePos++] = (byte)runLength;
                output[writePos++] = current;

                i += runLength;
            }

            return writePos;
        }

        /// <summary>
        /// Decodes Run-Length Encoded data.
        /// </summary>
        /// <param name="input">Encoded data.</param>
        /// <param name="inputLength">Number of encoded bytes to decode.</param>
        /// <param name="output">Destination buffer for decoded data.</param>
        /// <returns>Number of bytes written to output, or -1 if output is too small.</returns>
        public static int Decode(byte[] input, int inputLength, byte[] output)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));
            if (output == null) throw new ArgumentNullException(nameof(output));
            if (inputLength <= 0) return 0;

            int writePos = 0;

            for (int i = 0; i + 1 < inputLength; i += 2)
            {
                int runLength = input[i];
                byte value = input[i + 1];

                if (writePos + runLength > output.Length)
                    return -1;

                for (int j = 0; j < runLength; j++)
                {
                    output[writePos++] = value;
                }
            }

            return writePos;
        }

#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
        /// <summary>
        /// Encodes data using Run-Length Encoding (Span version).
        /// </summary>
        public static int Encode(ReadOnlySpan<byte> input, Span<byte> output)
        {
            if (input.IsEmpty) return 0;

            int writePos = 0;
            int i = 0;

            while (i < input.Length)
            {
                byte current = input[i];
                int runLength = 1;

                while (i + runLength < input.Length && input[i + runLength] == current && runLength < MaxRunLength)
                {
                    runLength++;
                }

                if (writePos + 2 > output.Length)
                    return -1;

                output[writePos++] = (byte)runLength;
                output[writePos++] = current;

                i += runLength;
            }

            return writePos;
        }

        /// <summary>
        /// Decodes Run-Length Encoded data (Span version).
        /// </summary>
        public static int Decode(ReadOnlySpan<byte> input, Span<byte> output)
        {
            if (input.IsEmpty) return 0;

            int writePos = 0;

            for (int i = 0; i + 1 < input.Length; i += 2)
            {
                int runLength = input[i];
                byte value = input[i + 1];

                if (writePos + runLength > output.Length)
                    return -1;

                output.Slice(writePos, runLength).Fill(value);
                writePos += runLength;
            }

            return writePos;
        }
#endif
    }
}
