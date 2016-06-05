/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 fx-highway (tools4j), Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.fx.highway.chronicle;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.NanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.octtech.bw.ByteWatcher;
import org.tools4j.fx.highway.util.SerializerHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(Parameterized.class)
public class ChronicleQueuePubSubTest {

    private final long messagesPerSecond;
    private final int numberOfBytes;

    private ChronicleQueue chronicleQueue;
    private ByteWatcher byteWatcher;

    @Parameterized.Parameters(name = "{index}: CH={0}, MPS={1}, NBYTES={2}")
    public static Collection testRunParameters() {
        return Arrays.asList(new Object[][] {
                { 160000, 100 },
                { 500000, 100 }
        });
    }

    public ChronicleQueuePubSubTest(final long messagesPerSecond,
                                    final int numberOfBytes) {
        this.messagesPerSecond = messagesPerSecond;
        this.numberOfBytes = numberOfBytes;
    }

    @Before
    public void setup() throws Exception {
        chronicleQueue = new ChronicleQueue();

        final long limit = 0;
        final Map<Thread, AtomicLong> lastSizePerThread = new ConcurrentHashMap<>();
        byteWatcher = new ByteWatcher();
        byteWatcher.onByteWatch((t, size) -> {
            final AtomicLong last = lastSizePerThread.computeIfAbsent(t, k -> new AtomicLong());
            if (last.getAndSet(size) < size) {
                System.out.printf("%s exceeded limit: %d using: %d%n",
                        t.getName(), limit, size);
            }
        } , limit);
    }

    @After
    public void tearDown() throws Exception {
        if (chronicleQueue != null) {
            chronicleQueue.close();
            chronicleQueue = null;
        }
        if (byteWatcher != null) {
            byteWatcher.shutdown();
            byteWatcher = null;
        }
    }

    @Test
    public void latencyTest() throws Exception {
        //given
        final long histogramMax = TimeUnit.SECONDS.toNanos(1);
        final int w = 200000;//warmup
        final int c = 1000000;//counted
        final int n = w+c;
        final long maxTimeToRunSeconds = 30;

        System.out.println("\twarmup + count      : " + w + " + " + c + " = " + n);
        System.out.println("\tmessagesPerSecond   : " + messagesPerSecond);
        System.out.println("\tmessageSize         : " + numberOfBytes + " bytes");
        System.out.println("\tmaxTimeToRunSeconds : " + maxTimeToRunSeconds);
        System.out.println();

        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = SerializerHelper.NANO_CLOCK;
        final Histogram histogram = new Histogram(1, histogramMax, 3);
        final CountDownLatch subscriberLatch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger();

        //when
        final Thread subscriberThread = new Thread(() -> {
            final ExcerptTailer tailer = chronicleQueue.getTailer();
            final AtomicLong t0 = new AtomicLong();
            final AtomicLong t1 = new AtomicLong();
            final AtomicLong t2 = new AtomicLong();
            while (!terminate.get()) {
                if (tailer.nextIndex()) {
                    if (count.get() == 0) t0.set(clock.nanoTime());
                    else if (count.get() == w-1) t1.set(clock.nanoTime());
                    else if (count.get() == n-1) t2.set(clock.nanoTime());
                    long sendTime = tailer.readLong();
                    for (int i = 8; i < numberOfBytes; ) {
                        if (i + 8 <= numberOfBytes) {
                            tailer.readLong();
                            i += 8;
                        } else {
                            tailer.readByte();
                            i++;
                        }
                    }
                    tailer.finish();
                    final long time = clock.nanoTime();
                    final int cnt = count.incrementAndGet();
                    if (cnt <= n) {
                        if (time - sendTime > histogramMax) {
                            //throw new RuntimeException("bad data in message " + cnt + ": time=" + time + ", sendTime=" + sendTime + ", dt=" + (time - sendTime));
                            histogram.recordValue(histogramMax);
                        } else {
                            histogram.recordValue(time - sendTime);
                        }
                    }
                    if (cnt == w) {
                        histogram.reset();
                    }
                    if (count.get() >= n) {
                        subscriberLatch.countDown();
                        break;
                    }
                }
            }
            System.out.println((t2.get() - t0.get())/1000.0 + " us total receiving time (" + (t2.get() - t1.get())/(1000f*c) + " us/message, " + c/((t2.get()-t1.get())/1000000000f) + " messages/second)");
        });
        subscriberThread.setName("subscriber-thread");
        subscriberThread.start();

        //publisher
        final Thread publisherThread = new Thread(() -> {
            final ExcerptAppender appender = chronicleQueue.getAppender();
            final long periodNs = 1000000000/messagesPerSecond;
            long cntAdmin = 0;
            long cntBackp = 0;
            long cnt = 0;
            final long t0 = clock.nanoTime();
            while (cnt < n && !terminate.get()) {
                long tCur = clock.nanoTime();
                while (tCur - t0 < cnt * periodNs) {
                    tCur = clock.nanoTime();
                }
                final long time = clock.nanoTime();
                appender.startExcerpt(numberOfBytes);
                appender.writeLong(time);
                for (int i = 8; i < numberOfBytes; ) {
                    if (i + 8 <= numberOfBytes) {
                        appender.writeLong(time + i);
                        i += 8;
                    } else {
                        appender.writeByte((byte)(time + i));
                        i++;
                    }
                }
                appender.finish();
                cnt++;
            }
            final long t1 = clock.nanoTime();
            System.out.println((t1 - t0) / 1000.0 + " us total publishing time (backp=" + cntBackp + ", admin=" + cntAdmin + ", cnt=" + cnt + ")");
        });
        publisherThread.setName("publisher-thread");
        publisherThread.start();;

        //then
        if (!subscriberLatch.await(maxTimeToRunSeconds, TimeUnit.SECONDS)) {
            terminate.set(true);
            System.err.println("timeout after receiving " + count + " messages.");
            throw new RuntimeException("simulation timed out");
        }
        terminate.set(true);

        publisherThread.join(2000);

        System.out.println();
        System.out.println("Percentiles (micros)");
        System.out.println("\t90%    : " + histogram.getValueAtPercentile(90)/1000f);
        System.out.println("\t99%    : " + histogram.getValueAtPercentile(99)/1000f);
        System.out.println("\t99.9%  : " + histogram.getValueAtPercentile(99.9)/1000f);
        System.out.println("\t99.99% : " + histogram.getValueAtPercentile(99.99)/1000f);
        System.out.println("\t99.999%: " + histogram.getValueAtPercentile(99.999)/1000f);
        System.out.println("\tmax    : " + histogram.getMaxValue()/1000f);
        System.out.println();
        System.out.println("Histogram (micros):");
        histogram.outputPercentileDistribution(System.out, 1000.0);
    }

    public static void main(String... args) throws Exception {
        final ChronicleQueuePubSubTest chronicleQueueLatencyTest = new ChronicleQueuePubSubTest(160000, 94);
        chronicleQueueLatencyTest.setup();
        try {
            chronicleQueueLatencyTest.latencyTest();
        } finally {
            chronicleQueueLatencyTest.tearDown();
        }
    }
}
