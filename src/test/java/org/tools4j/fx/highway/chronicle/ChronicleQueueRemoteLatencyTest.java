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
import org.tools4j.fx.highway.util.WaitLatch;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.tools4j.fx.highway.util.SerializerHelper.*;

@RunWith(Parameterized.class)
public class ChronicleQueueRemoteLatencyTest {

    private final String host;
    private final int port;
    private final long messagesPerSecond;
    private final int marketDataDepth;
    private final SupplierFactory<MarketDataSnapshotBuilder> builderSupplierFactory;

    private ChronicleSource chronicleSource;
//    private ChronicleSink chronicleSink;
    private ChronicleRemoteTailer remoteTailer;
//    private ChronicleRemoteAppender remoteAppender;
    private ByteWatcher byteWatcher;

    @Parameterized.Parameters(name = "{index}: HOST={0}, PORT={1}, MPS={2}, D={3}")
    public static Collection testRunParameters() {
        return Arrays.asList(new Object[][] {
                { "localhost", 55123, 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY },
                { "localhost", 55123, 500000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY }
        });
    }

    public ChronicleQueueRemoteLatencyTest(final String host,
                                           final int port,
                                           final long messagesPerSecond,
                                           final int marketDataDepth,
                                           final SupplierFactory<MarketDataSnapshotBuilder> builderSupplierFactory) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.messagesPerSecond = messagesPerSecond;
        this.marketDataDepth = marketDataDepth;
        this.builderSupplierFactory = Objects.requireNonNull(builderSupplierFactory);
    }

    @Before
    public void setup() throws Exception {
        chronicleSource = new ChronicleSource(host, port);
//        chronicleSink = new ChronicleSink(host, port);
//        remoteAppender = new ChronicleRemoteAppender(host, port);
        remoteTailer = new ChronicleRemoteTailer(host, port);

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
//        if (remoteAppender != null) {
//            remoteAppender.close();
//            remoteAppender= null;
//        }
        if (remoteTailer != null) {
            remoteTailer.close();
            remoteTailer = null;
        }
//        if (chronicleSink != null) {
//            chronicleSink.close();
//            chronicleSink = null;
//        }
        if (chronicleSource != null) {
            chronicleSource.close();
            chronicleSource = null;
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
        final int w = 200000;//warmup
        final int c = 100000;//counted
        final int n = w+c;
        final long maxTimeToRunSeconds = 15;
        final UnsafeBuffer sizeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

        System.out.println("\twarmup + count      : " + w + " + " + c + " = " + n);
        System.out.println("\tmessagesPerSecond   : " + messagesPerSecond);
        System.out.println("\tmarketDataDepth     : " + marketDataDepth);
        System.out.println("\tmessageSize         : " + encode(sizeBuf, givenMarketDataSnapshot(builder, marketDataDepth, marketDataDepth)) + " bytes");
        System.out.println("\tmaxTimeToRunSeconds : " + maxTimeToRunSeconds);
        System.out.println();

        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = SerializerHelper.NANO_CLOCK;
        final Histogram histogram = new Histogram(1, 100000000000L, 3);
        final WaitLatch pubSubReadyLatch = new WaitLatch(2);
        final WaitLatch receivedAllLatch = new WaitLatch(1);
        final AtomicInteger count = new AtomicInteger();

        //when
        final Thread subscriberThread = new Thread(() -> {
            final Supplier<MarketDataSnapshotBuilder> builderSupplier = builderSupplierFactory.create();
            final ExcerptTailer tailer = remoteTailer.getTailer();
            final AtomicLong t0 = new AtomicLong();
            final AtomicLong t1 = new AtomicLong();
            final AtomicLong t2 = new AtomicLong();
            final Consumer<UnsafeBuffer> consumer = (buf) -> {
                if (count.get() == 0) t0.set(clock.nanoTime());
                else if (count.get() == w-1) t1.set(clock.nanoTime());
                else if (count.get() == n-1) t2.set(clock.nanoTime());
                final MarketDataSnapshot decoded = decode(buf, builderSupplier.get());
                final long time = clock.nanoTime();
                final int cnt = count.incrementAndGet();
                if (cnt <= n) {
                    histogram.recordValue(time - decoded.getEventTimestamp());
                }
                if (cnt == w) {
                    histogram.reset();
                }
            };
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
            unsafeBuffer.wrap(byteBuffer);
            pubSubReadyLatch.countDown();
            while (!terminate.get()) {
                if (tailer.nextIndex()) {
                    final int len = tailer.length();
                    for (int i = 0; i < len; ) {
                        if (i + 8 <= len) {
                            byteBuffer.putLong(tailer.readLong());
                            i += 8;
                        } else {
                            byteBuffer.put(tailer.readByte());
                            i++;
                        }
                    }
                    byteBuffer.position(0);
                    tailer.finish();
                    consumer.accept(unsafeBuffer);
//                        byteBuffer.position(0);
                    if (count.get() >= n) {
                        receivedAllLatch.countDown();
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
            final Supplier<MarketDataSnapshotBuilder> builderSupplier = builderSupplierFactory.create();
//            final ExcerptAppender appender = remoteAppender.getAppender();
            final ExcerptAppender appender = chronicleSource.createAppender();
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
            unsafeBuffer.wrap(byteBuffer);
            final long periodNs = 1000000000/messagesPerSecond;
            pubSubReadyLatch.countDown();
            pubSubReadyLatch.awaitThrowOnTimeout(5, TimeUnit.SECONDS);
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
                appender.startExcerpt(len);
                try {
                    for (int i = 0; i < len; ) {
                        if (i + 8 <= len) {
                            appender.writeLong(byteBuffer.getLong());
                            i += 8;
                        } else {
                            appender.writeByte(byteBuffer.get());
                            i++;
                        }
                    }
                    byteBuffer.position(0);
                } catch (Exception e) {
                    throw new RuntimeException("msg " + cnt + " failed, e=" + e, e);
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
        if (!receivedAllLatch.await(maxTimeToRunSeconds, TimeUnit.SECONDS)) {
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
        final ChronicleQueueRemoteLatencyTest chronicleQueueLatencyTest = new ChronicleQueueRemoteLatencyTest("localhost", 1234, 160000, 2, MutableMarketDataSnapshot.BUILDER_SUPPLIER_FACTORY);
        chronicleQueueLatencyTest.setup();
        try {
            chronicleQueueLatencyTest.latencyTest();
        } finally {
            chronicleQueueLatencyTest.tearDown();
        }
    }
}
