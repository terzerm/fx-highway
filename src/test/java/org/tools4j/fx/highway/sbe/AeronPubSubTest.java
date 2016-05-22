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

import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tools4j.fx.highway.message.ImmutableMarketDataSnapshot;
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
                embeddedAeron.getSubscription().poll((buf, offset, len, header) -> {
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
            final long pubres = embeddedAeron.getPublication().offer(directBuffer, 0, len);
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
