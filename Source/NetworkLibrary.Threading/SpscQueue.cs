/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Threading - SpscQueue
 *
 *  Single-Producer Single-Consumer lock-free FIFO queue.
 *  Uses padding fields to avoid false sharing between producer and consumer threads.
 *  Perfect for game thread → network thread communication.
 */

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NetworkLibrary.Threading
{
    /// <summary>
    /// Single-Producer Single-Consumer lock-free FIFO queue.
    /// Wait-free for both enqueue and dequeue operations.
    /// Uses field padding to prevent false sharing.
    /// </summary>
    /// <typeparam name="T">Type of elements in the queue.</typeparam>
    public sealed class SpscQueue<T>
    {
        private readonly T[] _buffer;
        private readonly int _bufferMask;

        // Producer fields with cache-line padding
        private int _producerHead;
        private int _producerTail;

        // Padding to separate producer/consumer on different cache lines
        // 64 bytes = typical cache line. int = 4 bytes, so we need 14 padding ints
        private long _pad0, _pad1, _pad2, _pad3, _pad4, _pad5, _pad6;

        // Consumer fields
        private int _consumerHead;
        private int _consumerTail;

        /// <summary>
        /// Creates a new SPSC queue with the specified capacity (rounded up to next power of 2).
        /// </summary>
        public SpscQueue(int capacity)
        {
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            capacity = RoundUpPowerOf2(capacity);

            _buffer = new T[capacity];
            _bufferMask = capacity - 1;
            _producerHead = 0;
            _producerTail = 0;
            _consumerHead = 0;
            _consumerTail = 0;
        }

        /// <summary>The capacity of the queue.</summary>
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _buffer.Length;
        }

        /// <summary>Approximate number of items (for diagnostics only).</summary>
        public int Count
        {
            get
            {
                int head = Volatile.Read(ref _consumerHead);
                int tail = Volatile.Read(ref _producerTail);
                return tail - head;
            }
        }

        /// <summary>
        /// Tries to enqueue an item. Returns false if the queue is full.
        /// Must only be called from the producer thread.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnqueue(T item)
        {
            int head = _producerHead;
            int tail = _producerTail;
            int nextTail = tail + 1;

            if (nextTail - head > _bufferMask)
            {
                head = Volatile.Read(ref _consumerHead);
                _producerHead = head;

                if (nextTail - head > _bufferMask)
                    return false;
            }

            _buffer[tail & _bufferMask] = item;
            Volatile.Write(ref _producerTail, nextTail);
            return true;
        }

        /// <summary>
        /// Tries to dequeue an item. Returns false if the queue is empty.
        /// Must only be called from the consumer thread.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out T item)
        {
            int head = _consumerHead;
            int tail = _consumerTail;

            if (head == tail)
            {
                tail = Volatile.Read(ref _producerTail);
                _consumerTail = tail;

                if (head == tail)
                {
                    item = default!;
                    return false;
                }
            }

            item = _buffer[head & _bufferMask];
            _buffer[head & _bufferMask] = default!;
            Volatile.Write(ref _consumerHead, head + 1);
            return true;
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
    }
}
