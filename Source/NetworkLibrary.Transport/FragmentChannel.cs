/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - FragmentChannel
 *
 *  Handles fragmentation and reassembly of packets larger than MTU.
 *  Large packets are split into fragments, each sent as a separate UDP packet.
 *  Receiver reassembles them when all fragments arrive.
 */

using System;
using System.Runtime.CompilerServices;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Tracks the reassembly state of a fragmented packet.
    /// </summary>
    internal sealed class FragmentAssembly
    {
        public byte[]? Data;
        public bool[] ReceivedFragments;
        public int TotalFragments;
        public int ReceivedCount;
        public int TotalDataLength;
        public ushort Sequence;
        public bool Active;
        public long StartTimestamp;

        public FragmentAssembly(int maxFragments)
        {
            ReceivedFragments = new bool[maxFragments];
            Reset();
        }

        public void Reset()
        {
            TotalFragments = 0;
            ReceivedCount = 0;
            TotalDataLength = 0;
            Sequence = 0;
            Active = false;
            StartTimestamp = 0;

            if (Data != null)
            {
                ArrayPool<byte>.Shared.Return(Data);
                Data = null;
            }

            Array.Clear(ReceivedFragments, 0, ReceivedFragments.Length);
        }
    }

    /// <summary>
    /// Handles fragmentation of large packets and reassembly on the receiving end.
    /// Fragment header: [SequenceId: 2 bytes] [FragmentIndex: 1 byte] [TotalFragments: 1 byte]
    /// </summary>
    public sealed class FragmentChannel
    {
        /// <summary>Fragment header size in bytes (added to each fragment).</summary>
        public const int FragmentHeaderSize = 4;

        /// <summary>Maximum payload per fragment.</summary>
        public static readonly int FragmentPayloadSize = PacketHeader.MaxPayloadSize - FragmentHeaderSize;

        /// <summary>Maximum number of fragments per packet.</summary>
        public const int MaxFragments = 255;

        /// <summary>Maximum total data size for a fragmented packet.</summary>
        public static readonly int MaxFragmentedDataSize = FragmentPayloadSize * MaxFragments;

        /// <summary>Maximum number of simultaneous reassemblies in progress.</summary>
        private const int MaxPendingAssemblies = 64;
        private const int AssemblyMask = MaxPendingAssemblies - 1;

        /// <summary>Timeout for incomplete assemblies (10 seconds in Stopwatch ticks).</summary>
        private static readonly long AssemblyTimeout = System.Diagnostics.Stopwatch.Frequency * 10;

        private readonly FragmentAssembly[] _assemblies;

        public FragmentChannel()
        {
            // Lazy: assemblies are created on demand when the first fragment arrives.
            // Do NOT pre-allocate here — most peers will never use fragmentation.
            _assemblies = new FragmentAssembly[MaxPendingAssemblies];
        }

        /// <summary>
        /// Splits a large data packet into fragments.
        /// </summary>
        /// <param name="data">Source data to fragment.</param>
        /// <param name="offset">Offset in source data.</param>
        /// <param name="length">Length of source data.</param>
        /// <param name="sequenceId">Unique sequence ID for this fragmented packet.</param>
        /// <param name="fragments">Output array of fragment byte arrays.</param>
        /// <param name="fragmentLengths">Output array of fragment lengths.</param>
        /// <returns>Number of fragments created.</returns>
        public static int CreateFragments(byte[] data, int offset, int length, ushort sequenceId,
            byte[][] fragments, int[] fragmentLengths)
        {
            int totalFragments = (length + FragmentPayloadSize - 1) / FragmentPayloadSize;

            if (totalFragments > MaxFragments)
                throw new ArgumentException($"Data too large to fragment: {length} bytes requires {totalFragments} fragments (max {MaxFragments})");

            for (int i = 0; i < totalFragments; i++)
            {
                int fragOffset = i * FragmentPayloadSize;
                int fragLength = Math.Min(FragmentPayloadSize, length - fragOffset);
                int totalLength = FragmentHeaderSize + fragLength;

                if (fragments[i] == null || fragments[i].Length < totalLength)
                    fragments[i] = new byte[totalLength];

                // Write fragment header
                fragments[i][0] = (byte)(sequenceId & 0xFF);
                fragments[i][1] = (byte)(sequenceId >> 8);
                fragments[i][2] = (byte)i;
                fragments[i][3] = (byte)totalFragments;

                // Copy payload
                Buffer.BlockCopy(data, offset + fragOffset, fragments[i], FragmentHeaderSize, fragLength);

                fragmentLengths[i] = totalLength;
            }

            return totalFragments;
        }

        /// <summary>
        /// Processes a received fragment. Returns true and outputs the complete data
        /// when all fragments have been received.
        /// </summary>
        /// <param name="fragmentData">Raw fragment data including fragment header.</param>
        /// <param name="fragmentOffset">Offset in fragment data.</param>
        /// <param name="fragmentLength">Length of fragment data.</param>
        /// <param name="completeData">Output: reassembled complete data (only valid when returns true).</param>
        /// <param name="completeLength">Output: length of reassembled data.</param>
        /// <returns>True if all fragments received and data is complete.</returns>
        public bool ProcessFragment(byte[] fragmentData, int fragmentOffset, int fragmentLength,
            out byte[] completeData, out int completeLength)
        {
            completeData = null!;
            completeLength = 0;

            if (fragmentLength < FragmentHeaderSize)
                return false;

            // Read fragment header
            ushort sequenceId = (ushort)(fragmentData[fragmentOffset] | (fragmentData[fragmentOffset + 1] << 8));
            int fragmentIndex = fragmentData[fragmentOffset + 2];
            int totalFragments = fragmentData[fragmentOffset + 3];

            if (fragmentIndex >= totalFragments || totalFragments > MaxFragments)
                return false;

            int payloadLength = fragmentLength - FragmentHeaderSize;

            // Find or create assembly slot
            int slot = sequenceId & AssemblyMask;
            FragmentAssembly? assembly = _assemblies[slot];

            // Lazy-create the assembly object if it doesn't exist yet
            if (assembly == null)
            {
                assembly = new FragmentAssembly(MaxFragments);
                _assemblies[slot] = assembly;
            }

            // Check if this is a new fragmented packet or an existing one
            if (!assembly.Active || assembly.Sequence != sequenceId)
            {
                // New fragmented packet — reset the slot
                assembly.Reset();
                assembly.Data = ArrayPool<byte>.Shared.Rent(MaxFragmentedDataSize);
                assembly.Sequence = sequenceId;
                assembly.TotalFragments = totalFragments;
                assembly.Active = true;
                assembly.StartTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
            }

            // Validate consistency
            if (assembly.TotalFragments != totalFragments)
                return false;

            // Check for duplicate fragment
            if (assembly.ReceivedFragments[fragmentIndex])
                return false;

            // Copy fragment payload into reassembly buffer
            int dataOffset = fragmentIndex * FragmentPayloadSize;
            Buffer.BlockCopy(fragmentData, fragmentOffset + FragmentHeaderSize, assembly.Data!, dataOffset, payloadLength);

            assembly.ReceivedFragments[fragmentIndex] = true;
            assembly.ReceivedCount++;

            // Track total data length
            if (fragmentIndex == totalFragments - 1)
            {
                // Last fragment determines the total length
                assembly.TotalDataLength = dataOffset + payloadLength;
            }

            // Check if all fragments received
            if (assembly.ReceivedCount == assembly.TotalFragments && assembly.TotalDataLength > 0)
            {
                completeData = assembly.Data!;
                completeLength = assembly.TotalDataLength;
                
                assembly.Data = null; // Transfer ownership to caller
                assembly.Active = false;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Cleans up timed-out assemblies to prevent memory leaks.
        /// Should be called periodically (e.g., every second).
        /// </summary>
        public void CleanupTimedOut()
        {
            long now = System.Diagnostics.Stopwatch.GetTimestamp();

            for (int i = 0; i < MaxPendingAssemblies; i++)
            {
                // Skip slots that were never allocated (lazy)
                if (_assemblies[i] == null) continue;

                if (_assemblies[i].Active && (now - _assemblies[i].StartTimestamp) > AssemblyTimeout)
                {
                    _assemblies[i].Reset();
                }
            }
        }

        /// <summary>
        /// Checks if data needs fragmentation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool NeedsFragmentation(int dataLength)
        {
            return dataLength > PacketHeader.MaxPayloadSize;
        }
    }
}
