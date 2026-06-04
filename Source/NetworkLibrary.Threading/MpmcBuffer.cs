/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Threading - MpmcBuffer
 *
 *  Multi-Producer Multi-Consumer lock-free FIFO queue.
 *  Uses Interlocked CAS operations for thread safety.
 *  Uses padding fields to prevent false sharing.
 */

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace NetworkLibrary.Threading
{
    /// <summary>
    /// Multi-Producer Multi-Consumer lock-free bounded FIFO queue.
    /// Uses CAS (Compare-And-Swap) for non-blocking thread safety.
    /// </summary>
    /// <typeparam name="T">Type of elements in the buffer.</typeparam>
    public sealed class MpmcBuffer<T>
    {
        private readonly Cell[] _buffer;
        private readonly int _bufferMask;

        // Enqueue position with cache-line padding
        private int _enqueuePosition;
        private long _pad0, _pad1, _pad2, _pad3, _pad4, _pad5, _pad6;

        // Dequeue position with cache-line padding
        private int _dequeuePosition;
        private long _pad7, _pad8, _pad9, _pad10, _pad11, _pad12, _pad13;

        /// <summary>
        /// Creates a new MPMC buffer with the specified capacity (rounded up to next power of 2).
        /// </summary>
        public MpmcBuffer(int capacity)
        {
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            capacity = RoundUpPowerOf2(capacity);

            _bufferMask = capacity - 1;
            _buffer = new Cell[capacity];

            for (int i = 0; i < capacity; i++)
            {
                _buffer[i] = new Cell(i);
            }

            _enqueuePosition = 0;
            _dequeuePosition = 0;
        }

        /// <summary>The capacity of the buffer.</summary>
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _buffer.Length;
        }

        /// <summary>
        /// Tries to enqueue an item. Returns false if the buffer is full.
        /// Thread-safe for multiple producers.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(T item)
        {
            SpinWait spin = default;

            do
            {
                int position = Volatile.Read(ref _enqueuePosition);
                int index = position & _bufferMask;
                int sequence = Volatile.Read(ref _buffer[index].Sequence);
                int diff = sequence - position;

                if (diff == 0)
                {
                    if (Interlocked.CompareExchange(ref _enqueuePosition, position + 1, position) == position)
                    {
                        _buffer[index].Value = item;
                        Volatile.Write(ref _buffer[index].Sequence, position + 1);
                        return true;
                    }
                }
                else if (diff < 0)
                {
                    return false;
                }

                spin.SpinOnce();
            }
            while (true);
        }

        /// <summary>
        /// Tries to dequeue an item. Returns false if the buffer is empty.
        /// Thread-safe for multiple consumers.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out T item)
        {
            SpinWait spin = default;

            do
            {
                int position = Volatile.Read(ref _dequeuePosition);
                int index = position & _bufferMask;
                int sequence = Volatile.Read(ref _buffer[index].Sequence);
                int diff = sequence - (position + 1);

                if (diff == 0)
                {
                    if (Interlocked.CompareExchange(ref _dequeuePosition, position + 1, position) == position)
                    {
                        item = _buffer[index].Value;
                        _buffer[index].Value = default!;
                        Volatile.Write(ref _buffer[index].Sequence, position + _bufferMask + 1);
                        return true;
                    }
                }
                else if (diff < 0)
                {
                    item = default!;
                    return false;
                }

                spin.SpinOnce();
            }
            while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int RoundUpPowerOf2(int value)
        {
#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            return (int)BitOperations.RoundUpToPowerOf2((uint)value);
#else
            value--;
            value |= value >> 1;
            value |= value >> 2;
            value |= value >> 4;
            value |= value >> 8;
            value |= value >> 16;
            return value + 1;
#endif
        }

        private struct Cell
        {
            public int Sequence;
            public T Value;

            public Cell(int sequence)
            {
                Sequence = sequence;
                Value = default!;
            }
        }
    }
}
