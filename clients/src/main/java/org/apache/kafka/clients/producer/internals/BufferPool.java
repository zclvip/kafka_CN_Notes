/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
//只管理特定大小的ByteBuffer,如果要存放的消息的字节数大于该池子中的大小时，就会额外分配ByteBuffer,使用完成后直接丢弃掉
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    private final long totalMemory;//整个pool的大小
    private final int poolableSize;//buffer的大小
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;//缓存了指定大小的ByteBuffer对象
    private final Deque<Condition> waiters;//记录因申请不到足够的空间而阻塞的线程，实际记录的是阻塞线程对应的Condition对象
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "The total time an appender waits for space allocation.");
        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        ByteBuffer buffer = null;
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            //如果要申请的buffer大小 等于pool管理的buffer大小，并且free非空，则去第一个free中的第一个buffer
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            //计算free队列中所有buffer的大小
            int freeListSize = freeSize() * this.poolableSize;
            //如果可用的空间大于size，那么就进行分配空间 （this.nonPooledAvailableMemory + freeListSize 这个值<= totalMemory,因为buffer是在释放的时候保存进free中的）
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // we are out of memory and will have to block
                //走到这里说明，没有足够的空间了，需要进行等待
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            //调用condition的await方法进行超时等待，
                            //继续往下走的条件有两个，一个是调用了deallocate方法，该方法调用了signal方法唤醒线程；另一个是超时时间到了
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            this.waitTime.record(timeNs, time.milliseconds());
                        }

                        if (waitingTimeElapsed) {
                            throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        //如果是执行了deallocate方法，那么free队列中已经有了值，如果size=poolableSize 那么就从free获取buffer
                        //但是如果是await超时了，那么free可能为空，会走到else里面
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            //先释放一部分空间，继续等待，直到释放的空间满足申请大小要求
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    this.nonPooledAvailableMemory += accumulated;
                    //最终会删除condition
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                //如果有可用的空间，且有阻塞的线程，那么就取头部的线程唤醒
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    //分配内存
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error) {
                this.lock.lock();
                try {
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    //如果队列为空，不对free进行操作
    //如果剩余空间< size，则从free中从后往前释放空间
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            //从缓存队列中，从后往前是否内存，直到 nonPooledAvailableMemory > size
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    //释放内存，如果释放的buffer的大小时 poolableSize，则重置了buffer，然后放入free队列，已备重复使用
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                //如果不是管理的buffer大小，则nonPooledAvailableMemory进行累加
                this.nonPooledAvailableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                //condition调用signal唤醒等待的线程
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
