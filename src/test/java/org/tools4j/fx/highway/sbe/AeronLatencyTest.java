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
package org.tools4j.fx.highway.sbe;

import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tools4j.fx.highway.message.ImmutableMarketDataSnapshot;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.MarketDataSnapshotBuilder;
import org.tools4j.fx.highway.message.MutableMarketDataSnapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.tools4j.fx.highway.sbe.SerializerHelper.*;

public class AeronLatencyTest {

    private EmbeddedAeron embeddedAeron;

    @Before
    public void setup() {
        embeddedAeron = new EmbeddedAeron();
        embeddedAeron.awaitConnection(5, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        if (embeddedAeron != null) {
            embeddedAeron.shutdown();
            embeddedAeron = null;
        }
    }

    @Test
    public void latencyTestImmutable() throws Exception {
        latencyTest((s) -> new ImmutableMarketDataSnapshot.Builder(), () -> null);
    }

    @Test
    public void latencyTestMutable() throws Exception {
        latencyTest((s) -> s.builder(), () -> new MutableMarketDataSnapshot());
    }

    private <B> void latencyTest(final Function<B, MarketDataSnapshotBuilder> builderFunction, final Supplier<B> argumentSupplier) throws Exception {
        System.out.println("Using " + builderFunction.apply(argumentSupplier.get()).build().getClass().getSimpleName());
        //given
        final int w = 1000000;//warmup
        final int c = 200000;//counted
        final int n = w+c;
        final long messagesPerSecond = 160000;
        final long maxTimeToRunSeconds = 30;
        final int marketDataDepth = 2;

        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = new SystemNanoClock();
        final Histogram histogram = new Histogram(1, 1000000000, 3);
        final CountDownLatch subscriberLatch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger();

        //when
        final Thread subscriberThread = new Thread(() -> {
            final B builderArg = argumentSupplier.get();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
            final AtomicLong t0 = new AtomicLong();
            final AtomicLong t1 = new AtomicLong();
            final AtomicLong t2 = new AtomicLong();
            final FragmentHandler fh = (buf, offset, len, header) -> {
                if (count.get() == 0) t0.set(clock.nanoTime());
                else if (count.get() == w-1) t1.set(clock.nanoTime());
                else if (count.get() == n-1) t2.set(clock.nanoTime());
                unsafeBuffer.wrap(buf, offset, len);
                final MarketDataSnapshot decoded = decode(unsafeBuffer, builderFunction.apply(builderArg));
                final long time = clock.nanoTime();
                final int cnt = count.incrementAndGet();
                if (cnt <= n) {
                    histogram.recordValue(time - decoded.getEventTimestamp());
                }
                if (cnt == w) {
                    histogram.reset();
                }
            };
            while (!terminate.get()) {
                embeddedAeron.getSubscription().poll(fh, 256);
                if (count.get() >= n) {
                    subscriberLatch.countDown();
                    break;
                }
            }
            System.out.println((t2.get() - t0.get())/1000.0 + " us total receiving time (" + (t2.get() - t1.get())/(1000f*c) + " us/message, " + c/((t2.get()-t1.get())/1000000000f) + " messages/second)");
        });
        subscriberThread.start();

        //publisher
        final Runnable publisher = new Runnable() {
            final B builderArg = argumentSupplier.get();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
            long cntAdmin = 0;
            long cntBackp = 0;
            long cnt = 0;
            final long t0 = clock.nanoTime();
            boolean ended = false;
            public void run() {
                if (cnt >= n || terminate.get()) {
                    if (!ended) {
                        final long t1 = clock.nanoTime();
                        System.out.println((t1 - t0) / 1000.0 + " us total publishing time (backp=" + cntBackp + ", admin=" + cntAdmin + ", cnt=" + cnt + ")");
                        ended = true;
                    }
                    return;
                }
                final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(builderFunction.apply(builderArg), marketDataDepth, marketDataDepth);
                final int len = encode(unsafeBuffer, newSnapshot);
                long pubres;
                do {
                    pubres = embeddedAeron.getPublication().offer(unsafeBuffer, 0, len);
                    if (pubres < 0) {
                        if (pubres == Publication.BACK_PRESSURED) {
                            cntBackp++;
                        } else if (pubres == Publication.ADMIN_ACTION) {
                            cntAdmin++;
                        } else {
                            throw new RuntimeException("publication failed with pubres=" + pubres);
                        }
                        Thread.yield();
                    }
                } while (pubres < 0);
            }
        };
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(publisher, 0, 1000000000/messagesPerSecond, TimeUnit.NANOSECONDS);

        //then
        if (!subscriberLatch.await(maxTimeToRunSeconds, TimeUnit.SECONDS)) {
            terminate.set(true);
            System.err.println("timeout after receiving " + count + " messages.");
            throw new RuntimeException("simulation timed out");
        }
        terminate.set(true);

        scheduler.shutdown();
        scheduler.awaitTermination(2, TimeUnit.SECONDS);
        scheduler.shutdownNow();

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
}
