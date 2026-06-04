/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Buffers
 *
 *  Thread-safe array pool to eliminate GC allocations on hot paths.
 *  Arrays are bucketed by power-of-2 sizes for O(1) rent/return.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NetworkLibrary.Buffers
{
    /// <summary>
    /// Thread-safe pool of reusable arrays, bucketed by power-of-2 sizes.
    /// Eliminates GC pressure from frequent buffer allocations in networking code.
    /// </summary>
    public sealed class ArrayPool<T>
    {
        private const int DefaultMaxArrayLength = 1024 * 1024; // 1 MB
        private const int DefaultMaxArraysPerBucket = 50;
        private const int MinBucketIndex = 4; // 16 bytes minimum

        private readonly Bucket[] _buckets;
        private readonly int _maxArrayLength;

        /// <summary>
        /// Creates a new ArrayPool with specified limits.
        /// </summary>
        /// <param name="maxArrayLength">Maximum length of arrays the pool will cache.</param>
        /// <param name="maxArraysPerBucket">Maximum number of arrays stored per size bucket.</param>
        public ArrayPool(int maxArrayLength = DefaultMaxArrayLength, int maxArraysPerBucket = DefaultMaxArraysPerBucket)
        {
            if (maxArrayLength <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxArrayLength));
            if (maxArraysPerBucket <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxArraysPerBucket));

            _maxArrayLength = maxArrayLength;

            int maxBucketIndex = SelectBucketIndex(maxArrayLength);
            int bucketCount = maxBucketIndex - MinBucketIndex + 1;

            _buckets = new Bucket[bucketCount];

            for (int i = 0; i < bucketCount; i++)
            {
                int bucketSize = GetMaxSizeForBucket(i);
                _buckets[i] = new Bucket(bucketSize, maxArraysPerBucket);
            }
        }

        /// <summary>
        /// Rents a buffer from the pool with at least the specified minimum length.
        /// The returned buffer may be larger than requested.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] Rent(int minimumLength)
        {
            if (minimumLength < 0)
                throw new ArgumentOutOfRangeException(nameof(minimumLength));

            if (minimumLength == 0)
                return Array.Empty<T>();

            int bucketIndex = SelectBucketIndex(minimumLength) - MinBucketIndex;

            if (bucketIndex >= 0 && bucketIndex < _buckets.Length)
            {
                T[]? buffer = _buckets[bucketIndex].TryPop();

                if (buffer != null)
                    return buffer;

                return new T[_buckets[bucketIndex].BufferLength];
            }

            // Too large for the pool — allocate directly
            return new T[minimumLength];
        }

        /// <summary>
        /// Returns a previously rented buffer back to the pool.
        /// </summary>
        /// <param name="array">The array to return.</param>
        /// <param name="clearArray">If true, clears the array contents before returning.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(T[] array, bool clearArray = false)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (array.Length == 0)
                return;

            if (clearArray)
                Array.Clear(array, 0, array.Length);

            int bucketIndex = SelectBucketIndex(array.Length) - MinBucketIndex;

            if (bucketIndex >= 0 && bucketIndex < _buckets.Length)
            {
                _buckets[bucketIndex].TryPush(array);
            }
            // If too large, just let GC collect it
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SelectBucketIndex(int length)
        {
            // Find the next power of 2 bucket
#if NET10_0_OR_GREATER || NET6_0_OR_GREATER
            return Math.Max(MinBucketIndex, 32 - int.LeadingZeroCount(length - 1));
#else
            int bits = 0;
            int v = length - 1;
            while (v > 0)
            {
                bits++;
                v >>= 1;
            }
            return Math.Max(MinBucketIndex, bits);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetMaxSizeForBucket(int bucketIndex)
        {
            return 1 << (bucketIndex + MinBucketIndex);
        }

        /// <summary>
        /// A single bucket holding arrays of a specific size.
        /// Uses SpinLock for minimal overhead thread safety.
        /// </summary>
        private sealed class Bucket
        {
            private readonly T[][] _buffers;
            private readonly int _bufferLength;
            private int _count;
            private SpinLock _lock;

            public int BufferLength => _bufferLength;

            public Bucket(int bufferLength, int maxBuffers)
            {
                _bufferLength = bufferLength;
                _buffers = new T[maxBuffers][];
                _count = 0;
                _lock = new SpinLock(enableThreadOwnerTracking: false);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T[]? TryPop()
            {
                T[]? buffer = null;
                bool lockTaken = false;

                try
                {
                    _lock.Enter(ref lockTaken);

                    if (_count > 0)
                    {
                        _count--;
                        buffer = _buffers[_count];
                        _buffers[_count] = null!;
                    }
                }
                finally
                {
                    if (lockTaken)
                        _lock.Exit(useMemoryBarrier: false);
                }

                return buffer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TryPush(T[] array)
            {
                bool lockTaken = false;

                try
                {
                    _lock.Enter(ref lockTaken);

                    if (_count < _buffers.Length)
                    {
                        _buffers[_count] = array;
                        _count++;
                    }
                }
                finally
                {
                    if (lockTaken)
                        _lock.Exit(useMemoryBarrier: false);
                }
            }
        }

        // --- Shared default instance ---

        private static ArrayPool<T>? _shared;

        /// <summary>
        /// Gets a shared ArrayPool instance. Thread-safe, lazily initialized.
        /// </summary>
        public static ArrayPool<T> Shared
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _shared ?? EnsureSharedCreated();
        }

        private static ArrayPool<T> EnsureSharedCreated()
        {
            Interlocked.CompareExchange(ref _shared, new ArrayPool<T>(), null);
            return _shared!;
        }
    }
}
