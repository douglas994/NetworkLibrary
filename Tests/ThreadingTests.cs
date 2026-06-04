using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using NetworkLibrary.Threading;
using NetworkLibrary.Buffers;

namespace NetworkLibrary.Tests
{
    public class ThreadingTests
    {
        // ═══════════════════════════════════════════════════════
        //  SPSC QUEUE
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void SpscQueue_BasicOperations()
        {
            var queue = new SpscQueue<int>(8);

            Assert.True(queue.TryEnqueue(1));
            Assert.True(queue.TryEnqueue(2));
            Assert.True(queue.TryEnqueue(3));

            Assert.True(queue.TryDequeue(out int v1));
            Assert.Equal(1, v1);

            Assert.True(queue.TryDequeue(out int v2));
            Assert.Equal(2, v2);

            Assert.True(queue.TryDequeue(out int v3));
            Assert.Equal(3, v3);

            Assert.False(queue.TryDequeue(out _));
        }

        [Fact]
        public void SpscQueue_Full()
        {
            // Capacity 4 = power of 2, mask = 3
            // Circular buffer reserves 1 slot to distinguish full from empty
            // So usable capacity is mask (3) items
            var queue = new SpscQueue<int>(4);

            // Fill to usable capacity
            Assert.True(queue.TryEnqueue(1));
            Assert.True(queue.TryEnqueue(2));
            Assert.True(queue.TryEnqueue(3));

            // 4th enqueue on capacity-4 queue should fail (1 slot reserved)
            Assert.False(queue.TryEnqueue(4));

            // Dequeue one, should allow enqueue
            Assert.True(queue.TryDequeue(out int val));
            Assert.Equal(1, val);
            Assert.True(queue.TryEnqueue(4));
        }

        [Fact]
        public void SpscQueue_ConcurrentProducerConsumer()
        {
            const int count = 100_000;
            var queue = new SpscQueue<int>(1024);
            int consumed = 0;
            long sum = 0;

            var producer = Task.Run(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    while (!queue.TryEnqueue(i))
                    {
                        Thread.SpinWait(1);
                    }
                }
            });

            var consumer = Task.Run(() =>
            {
                int expected = 0;

                while (expected < count)
                {
                    if (queue.TryDequeue(out int value))
                    {
                        Assert.Equal(expected, value);
                        sum += value;
                        expected++;
                        consumed++;
                    }
                    else
                    {
                        Thread.SpinWait(1);
                    }
                }
            });

            Task.WaitAll(producer, consumer);

            Assert.Equal(count, consumed);

            // Verify sum: 0 + 1 + 2 + ... + (count-1) = count*(count-1)/2
            long expectedSum = (long)count * (count - 1) / 2;
            Assert.Equal(expectedSum, sum);
        }

        // ═══════════════════════════════════════════════════════
        //  MPMC BUFFER
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void MpmcBuffer_BasicOperations()
        {
            var buffer = new MpmcBuffer<int>(8);

            Assert.True(buffer.TryEnqueue(10));
            Assert.True(buffer.TryEnqueue(20));
            Assert.True(buffer.TryEnqueue(30));

            Assert.True(buffer.TryDequeue(out int v1));
            Assert.Equal(10, v1);

            Assert.True(buffer.TryDequeue(out int v2));
            Assert.Equal(20, v2);

            Assert.True(buffer.TryDequeue(out int v3));
            Assert.Equal(30, v3);

            Assert.False(buffer.TryDequeue(out _));
        }

        [Fact]
        public void MpmcBuffer_MultiProducerMultiConsumer()
        {
            const int producerCount = 4;
            const int itemsPerProducer = 10_000;
            var buffer = new MpmcBuffer<int>(4096);
            int totalConsumed = 0;

            var producers = new Task[producerCount];
            for (int p = 0; p < producerCount; p++)
            {
                int producerIndex = p;
                producers[p] = Task.Run(() =>
                {
                    for (int i = 0; i < itemsPerProducer; i++)
                    {
                        int value = producerIndex * itemsPerProducer + i;
                        while (!buffer.TryEnqueue(value))
                        {
                            Thread.SpinWait(1);
                        }
                    }
                });
            }

            var consumers = new Task[2];
            for (int c = 0; c < 2; c++)
            {
                consumers[c] = Task.Run(() =>
                {
                    int count = 0;
                    while (count < producerCount * itemsPerProducer / 2)
                    {
                        if (buffer.TryDequeue(out _))
                        {
                            count++;
                            Interlocked.Increment(ref totalConsumed);
                        }
                        else
                        {
                            Thread.SpinWait(1);
                        }
                    }
                });
            }

            Task.WaitAll(producers);
            Task.WaitAll(consumers);

            Assert.Equal(producerCount * itemsPerProducer, totalConsumed);
        }

        // ═══════════════════════════════════════════════════════
        //  CONCURRENT POOL
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void ConcurrentPool_AcquireRelease()
        {
            int created = 0;
            var pool = new ConcurrentPool<object>(4, () =>
            {
                Interlocked.Increment(ref created);
                return new object();
            });

            Assert.Equal(4, created); // Pre-allocated

            var obj1 = pool.Acquire();
            var obj2 = pool.Acquire();

            Assert.NotNull(obj1);
            Assert.NotNull(obj2);
            Assert.NotSame(obj1, obj2);

            pool.Release(obj1);
            var obj3 = pool.Acquire();

            Assert.Same(obj1, obj3); // Should reuse the released object
        }

        [Fact]
        public void ConcurrentPool_GrowsOnDemand()
        {
            int created = 0;
            var pool = new ConcurrentPool<object>(2, () =>
            {
                Interlocked.Increment(ref created);
                return new object();
            });

            Assert.Equal(2, created);

            // Acquire all pre-allocated
            pool.Acquire();
            pool.Acquire();

            // Acquire one more — should create via factory
            var extra = pool.Acquire();
            Assert.NotNull(extra);
            Assert.Equal(3, created);
        }

        // ═══════════════════════════════════════════════════════
        //  ARRAY POOL
        // ═══════════════════════════════════════════════════════

        [Fact]
        public void ArrayPool_RentReturn()
        {
            var pool = new ArrayPool<byte>();

            byte[] buffer = pool.Rent(64);
            Assert.True(buffer.Length >= 64);

            pool.Return(buffer);

            // Renting same size should return pooled buffer
            byte[] buffer2 = pool.Rent(64);
            Assert.Same(buffer, buffer2);
        }

        [Fact]
        public void ArrayPool_RentLargerThanRequested()
        {
            var pool = new ArrayPool<byte>();

            // Requesting 100 bytes should give 128 (next power of 2)
            byte[] buffer = pool.Rent(100);
            Assert.True(buffer.Length >= 100);
            Assert.Equal(128, buffer.Length);
        }

        [Fact]
        public void ArrayPool_ClearOnReturn()
        {
            var pool = new ArrayPool<byte>();

            byte[] buffer = pool.Rent(16);
            buffer[0] = 42;
            buffer[1] = 99;

            pool.Return(buffer, clearArray: true);

            byte[] buffer2 = pool.Rent(16);
            Assert.Equal(0, buffer2[0]);
            Assert.Equal(0, buffer2[1]);
        }

        [Fact]
        public void ArrayPool_Shared()
        {
            var shared1 = ArrayPool<byte>.Shared;
            var shared2 = ArrayPool<byte>.Shared;

            Assert.Same(shared1, shared2);
        }
    }
}
