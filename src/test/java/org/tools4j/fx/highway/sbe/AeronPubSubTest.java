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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tools4j.fx.highway.message.MarketDataSnapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot();
        final BlockingQueue<MarketDataSnapshot> queue = new ArrayBlockingQueue<MarketDataSnapshot>(1);
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
                        final MarketDataSnapshot decoded = decode(directBuffer);
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

}
