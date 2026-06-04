/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Threading - ConcurrentPool
 *
 *  Thread-safe object pool using SpinLock.
 *  Uses segmented storage (linked list of arrays) for growth without reallocation.
 *  Perfect for pooling packet objects, message objects, etc.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NetworkLibrary.Threading
{
    /// <summary>
    /// Thread-safe object pool with factory-based creation.
    /// Uses segmented storage to grow without reallocating existing arrays.
    /// </summary>
    /// <typeparam name="T">Type of pooled objects (must be a reference type).</typeparam>
    public sealed class ConcurrentPool<T> where T : class
    {
        private readonly Func<T> _factory;
        private SpinLock _lock;
        private Segment _head;
        private Segment _tail;

        /// <summary>
        /// Creates a new ConcurrentPool with the specified initial capacity.
        /// </summary>
        /// <param name="capacity">Initial number of pre-allocated objects.</param>
        /// <param name="factory">Factory function to create new objects.</param>
        public ConcurrentPool(int capacity, Func<T> factory)
        {
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));
            if (factory == null)
                throw new ArgumentNullException(nameof(factory));

            _factory = factory;
            _lock = new SpinLock(enableThreadOwnerTracking: false);

            // Pre-allocate the initial segment
            _head = new Segment(capacity);
            _tail = _head;

            // Pre-populate with objects
            for (int i = 0; i < capacity; i++)
            {
                _head.Items[i] = factory();
            }

            _head.Count = capacity;
        }

        /// <summary>
        /// Acquires an object from the pool. If the pool is empty, creates a new object via factory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Acquire()
        {
            T? item = null;
            bool lockTaken = false;

            try
            {
                _lock.Enter(ref lockTaken);

                Segment? segment = _head;

                while (segment != null)
                {
                    if (segment.Count > 0)
                    {
                        segment.Count--;
                        item = segment.Items[segment.Count];
                        segment.Items[segment.Count] = null!;
                        break;
                    }

                    segment = segment.Next;
                }
            }
            finally
            {
                if (lockTaken)
                    _lock.Exit(useMemoryBarrier: false);
            }

            return item ?? _factory();
        }

        /// <summary>
        /// Returns an object to the pool for future reuse.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(T item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            bool lockTaken = false;

            try
            {
                _lock.Enter(ref lockTaken);

                // Try to find space in existing segments
                Segment? segment = _head;

                while (segment != null)
                {
                    if (segment.Count < segment.Items.Length)
                    {
                        segment.Items[segment.Count] = item;
                        segment.Count++;
                        return;
                    }

                    segment = segment.Next;
                }

                // All segments full — grow by adding a new segment
                var newSegment = new Segment(_head.Items.Length);
                newSegment.Items[0] = item;
                newSegment.Count = 1;

                _tail.Next = newSegment;
                _tail = newSegment;
            }
            finally
            {
                if (lockTaken)
                    _lock.Exit(useMemoryBarrier: false);
            }
        }

        /// <summary>
        /// A segment in the linked list of object arrays.
        /// </summary>
        private sealed class Segment
        {
            public readonly T[] Items;
            public int Count;
            public Segment? Next;

            public Segment(int capacity)
            {
                Items = new T[capacity];
                Count = 0;
                Next = null;
            }
        }
    }
}
