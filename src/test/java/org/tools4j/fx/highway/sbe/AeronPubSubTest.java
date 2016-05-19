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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
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
import org.tools4j.fx.highway.message.MutableMarketDataSnapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.tools4j.fx.highway.sbe.SerializerHelper.*;

public class AeronPubSubTest {

    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Subscription subscription;
    private Publication publication;

    @Before
    public void setup() {
        mediaDriver = MediaDriver.launchEmbedded();
        final Aeron.Context context = new Aeron.Context();
        context.aeronDirectoryName(mediaDriver.aeronDirectoryName());
        aeron = Aeron.connect(context);
        subscription = aeron.addSubscription("udp://localhost:40123", 10);
        publication = aeron.addPublication("udp://localhost:40123", 10);
        int cnt = 0;
        while (!publication.isConnected()) {
            if (cnt > 20) {
                throw new RuntimeException("publisher not connected");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            cnt++;
        }
    }

    @After
    public void tearDown() {
        publication.close();
        subscription.close();
        aeron.close();
        mediaDriver.close();
    }

    @Test
    public void subscriptionShouldReceivePublishedSnapshot() throws Exception {
        //given
        final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(new ImmutableMarketDataSnapshot.Builder());
        final BlockingQueue<MarketDataSnapshot> queue = new ArrayBlockingQueue<>(1);
        final CountDownLatch subscriberStarted = new CountDownLatch(1);
        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = new SystemNanoClock();

        //when
        final Thread subscriberThread = new Thread(() -> {
            subscriberStarted.countDown();
            System.out.println(clock.nanoTime() + " subscriber started");
            while (!terminate.get()) {
                subscription.poll((buf, offset, len, header) -> {
                    try {
                        System.out.println(clock.nanoTime() + " poll called, len=" + len);
                        final UnsafeBuffer directBuffer = new UnsafeBuffer(buf, offset, len);
                        final MarketDataSnapshot decoded = decode(directBuffer, new ImmutableMarketDataSnapshot.Builder());
                        queue.add(decoded);
                        System.out.println(clock.nanoTime() + " decoded: " + decoded);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, 10);
            }
            System.out.println(clock.nanoTime() + " subscriber thread terminating");
        });
        subscriberThread.start();
        if (!subscriberStarted.await(2, TimeUnit.SECONDS)) {
            throw new RuntimeException("subscriber not started");
        }
        final Thread publisherThread = new Thread(() -> {
            System.out.println(clock.nanoTime() + " publisher thread started");
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
            final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
            final int len = encode(directBuffer, newSnapshot);
//            publication.offer(directBuffer);
            final long pubres = publication.offer(directBuffer, 0, len);
            System.out.println(clock.nanoTime() + " published, res=" + pubres + ", len=" + len);
            System.out.println(clock.nanoTime() + " publisher thread terminating");
        });
        publisherThread.start();

        //then
        final MarketDataSnapshot decoded = queue.poll(2, TimeUnit.SECONDS);
        terminate.set(true);

        publisherThread.join();
        subscriberThread.join();

        assertThat(decoded).isEqualTo(newSnapshot);
     }

    @Test
    public void histogramTest() throws Exception {
        //given
        final int w = 1000000;//warmup
        final int c = 1000000;//counted
        final int n = w+c;
        final long maxTimeToRunSeconds = 10;
        final AtomicBoolean terminate = new AtomicBoolean(false);
        final NanoClock clock = new SystemNanoClock();
        final Histogram histogram = new Histogram(1, 1000000000, 3);
        final CountDownLatch subscriberLatch = new CountDownLatch(n);

        //when
        final Thread subscriberThread = new Thread(() -> {
            final MutableMarketDataSnapshot marketDataSnapshot = new MutableMarketDataSnapshot();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
            final AtomicInteger count = new AtomicInteger();
            final AtomicLong t0 = new AtomicLong();
            final AtomicLong t1 = new AtomicLong();
            final FragmentHandler fh = (buf, offset, len, header) -> {
                if (count.get() == 0) t0.set(clock.nanoTime());
                else if (count.get() == n-1) t1.set(clock.nanoTime());
                unsafeBuffer.wrap(buf, offset, len);
                final MarketDataSnapshot decoded = decode(unsafeBuffer, marketDataSnapshot.builder());
                final long time = clock.nanoTime();
                if (count.incrementAndGet() >= w) {
                    histogram.recordValue(time - decoded.getEventTimestamp());
                    if (count.get() - 10 < w) {
                        System.out.println("c " + System.nanoTime() + ": " + time + " - " + decoded.getEventTimestamp() + ":\t" + (time - decoded.getEventTimestamp())/1000.0 + " us");
                    }
                }
                subscriberLatch.countDown();
            };
            while (!terminate.get()) {
                subscription.poll(fh, 10);
            }
            System.out.println((t1.get() - t0.get())/1000.0 + " us total receiving time");
        });
        subscriberThread.start();

        //publisher
        {
            final MutableMarketDataSnapshot marketDataSnapshot = new MutableMarketDataSnapshot();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
            final long t0 = clock.nanoTime();
            for (int i = 0; i < n && !terminate.get(); i++) {
                final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(marketDataSnapshot.builder());
                final int len = encode(unsafeBuffer, newSnapshot);
                long pubres;
                do {
                    pubres = publication.offer(unsafeBuffer, 0, len);
                    if (pubres < 0) {
                        if (pubres != Publication.BACK_PRESSURED && pubres != Publication.ADMIN_ACTION) {
                            throw new RuntimeException("publication failed with pubres=" + pubres);
                        }
                    }
                } while (pubres < 0);
            }
            final long t1 = clock.nanoTime();
            System.out.println((t1 - t0) / 1000.0 + " us total publishing time");
        }

        //then
        if (!subscriberLatch.await(maxTimeToRunSeconds, TimeUnit.SECONDS)) {
            terminate.set(true);
            throw new RuntimeException("simulation timed out");
        }
        terminate.set(true);

        subscriberThread.join();

        System.out.println("Histogram (micros):");
        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
