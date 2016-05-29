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
package org.tools4j.fx.highway.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.octtech.bw.ByteWatcher;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.MarketDataSnapshotBuilder;
import org.tools4j.fx.highway.message.MutableMarketDataSnapshot;
import org.tools4j.fx.highway.message.SupplierFactory;
import org.tools4j.fx.highway.util.SerializerHelper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.tools4j.fx.highway.util.SerializerHelper.*;

@RunWith(Parameterized.class)
public class AeronEmbeddedLatencyTest {

    private final String channel;
    private final long messagesPerSecond;
    private final int marketDataDepth;
    private final SupplierFactory<MarketDataSnapshotBuilder> builderSupplierFactory;

    private EmbeddedAeron embeddedAeron;
    private ByteWatcher byteWatcher;

    @Parameterized.Parameters(name = "{index}: CH={0}, MPS={1}, D={2}")
    public static Collection testRunParameters() {
        return Arrays.asList(new Object[][] {
                { "aeron:ipc", 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY },
                { "aeron:ipc", 500000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY },
                { "udp://localhost:40123", 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY },
                { "udp://224.10.9.7:4050", 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY}
        });
    }

    public AeronEmbeddedLatencyTest(final String channel,
                                    final long messagesPerSecond,
                                    final int marketDataDepth,
                                    final SupplierFactory<MarketDataSnapshotBuilder> builderSupplierFactory) {
        this.channel = Objects.requireNonNull(channel);
        this.messagesPerSecond = messagesPerSecond;
        this.marketDataDepth = marketDataDepth;
        this.builderSupplierFactory = Objects.requireNonNull(builderSupplierFactory);
    }

    @Before
    public void setup() {
        embeddedAeron = new EmbeddedAeron(channel, 10);
        embeddedAeron.awaitConnection(5, TimeUnit.SECONDS);

        final long limit = 1<<20;
        byteWatcher = new ByteWatcher();
        byteWatcher.onByteWatch((t, size) ->
                        System.out.printf("%s exceeded limit: %d using: %d%n",
                                t.getName(), limit, size)
                , limit);
    }

    @After
    public void tearDown() {
        if (embeddedAeron != null) {
            embeddedAeron.shutdown();
            embeddedAeron = null;
        }
        if (byteWatcher != null) {
            byteWatcher.shutdown();
            byteWatcher = null;
        }
    }

    @Test
    public void latencyTest() throws Exception {
        final MarketDataSnapshotBuilder builder = builderSupplierFactory.create().get();
        System.out.println("Using " + builder.build().getClass().getSimpleName());
        //given
        final int w = 1000000;//warmup
        final int c = 200000;//counted
        final int n = w+c;
        final long maxTimeToRunSeconds = 60;
        final UnsafeBuffer sizeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

        System.out.println("\tchannel             : " + channel);
        System.out.println("\twarmup + count      : " + w + " + " + c + " = " + n);
        System.out.println("\tmessagesPerSecond   : " + messagesPerSecond);
        System.out.println("\tmarketDataDepth     : " + marketDataDepth);
        System.out.println("\tmessageSize         : " + encode(sizeBuf, givenMarketDataSnapshot(builder, marketDataDepth, marketDataDepth)) + " bytes");
        System.out.println("\tmaxTimeToRunSeconds : " + maxTimeToRunSeconds);
        System.out.println();

        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = SerializerHelper.NANO_CLOCK;
        final Histogram histogram = new Histogram(1, 1000000000, 3);
        final CountDownLatch subscriberLatch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger();

        //when
        final Thread subscriberThread = new Thread(() -> {
            final Supplier<MarketDataSnapshotBuilder> builderSupplier = builderSupplierFactory.create();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
            final AtomicLong t0 = new AtomicLong();
            final AtomicLong t1 = new AtomicLong();
            final AtomicLong t2 = new AtomicLong();
            final FragmentHandler fh = (buf, offset, len, header) -> {
                if (count.get() == 0) t0.set(clock.nanoTime());
                else if (count.get() == w-1) t1.set(clock.nanoTime());
                else if (count.get() == n-1) t2.set(clock.nanoTime());
                unsafeBuffer.wrap(buf, offset, len);
                final MarketDataSnapshot decoded = decode(unsafeBuffer, builderSupplier.get());
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
        final Thread publisherThread = new Thread(() -> {
            final Supplier<MarketDataSnapshotBuilder> builderSupplier = builderSupplierFactory.create();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
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
                final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(builderSupplier.get(), marketDataDepth, marketDataDepth);
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
        final AeronEmbeddedLatencyTest aeronLatencyTest = new AeronEmbeddedLatencyTest("udp://224.10.9.7:4050", 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY);
        aeronLatencyTest.setup();
        try {
            aeronLatencyTest.latencyTest();
        } finally {
            aeronLatencyTest.tearDown();
        }
    }
}
