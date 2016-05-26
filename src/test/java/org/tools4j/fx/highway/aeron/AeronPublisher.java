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

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.MutableMarketDataSnapshot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.tools4j.fx.highway.sbe.SerializerHelper.encode;
import static org.tools4j.fx.highway.sbe.SerializerHelper.givenMarketDataSnapshot;

/**
 * Starts the aeron publisher in a separate process.
 */
public class AeronPublisher extends AbstractAeronProcess {

    private final String channel;
    private final int streamId;
    private final long messageCount;
    private final long messagesPerSecond;
    private final int marketDataDepth;

    public AeronPublisher(final String aeronDirectoryName,
                          final long messageCount, final long messagesPerSecond,
                          final int marketDataDepth) {
        this(aeronDirectoryName, "udp://localhost:40123", 10,
                messageCount, messagesPerSecond, marketDataDepth);
    }

    public AeronPublisher(final String aeronDirectoryName,
                          final String channel, final int streamId,
                          final long messageCount, final long messagesPerSecond,
                          final int marketDataDepth) {
        super(aeronDirectoryName);
        this.channel = Objects.requireNonNull(channel);
        this.streamId = streamId;
        this.messageCount = messageCount;
        this.messagesPerSecond = messagesPerSecond;
        this.marketDataDepth = marketDataDepth;
    }

    public String getChannel() {
        return channel;
    }

    public int getStreamId() {
        return streamId;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public long getMessagesPerSecond() {
        return messagesPerSecond;
    }

    public int getMarketDataDepth() {
        return marketDataDepth;
    }

    public void start() {
        super.start(AeronPublisher.class);
    }

    @Override
    protected List<String> mainArgs() {
        final List<String> args = new ArrayList<>();
        args.add(getAeronDirectoryName());
        args.add(getChannel());
        args.add(String.valueOf(getStreamId()));
        args.add(String.valueOf(getMessageCount()));
        args.add(String.valueOf(getMessagesPerSecond()));
        args.add(String.valueOf(getMarketDataDepth()));
        return args;
    }

    public static void main(final String... args) {
        final String aeronDirectoryName = args[0];
        final String channel = args[1];
        final int streamId = Integer.parseInt(args[2]);
        final long messageCount = Long.parseLong(args[3]);
        final long messagesPerSecond = Long.parseLong(args[4]);
        final int marketDataDepth = Integer.parseInt(args[5]);

        final Aeron aeron = aeron(aeronDirectoryName);
        final Publication publication = aeron.addPublication(channel, streamId);
        try {
            awaitConnection(publication, 5, TimeUnit.SECONDS);
            run(publication, messageCount, messagesPerSecond, marketDataDepth);
        } finally {
            publication.close();
            aeron.close();
            System.out.println("Shutdown " + AeronPublisher.class.getSimpleName() + "...");
        }
    }

    private static void run(final Publication publication, final long messageCount, final long messagesPerSecond, final int marketDataDepth) {
        final NanoClock clock = new SystemNanoClock();
        final MutableMarketDataSnapshot snapshot = new MutableMarketDataSnapshot();
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
        final long periodNs = 1000000000/messagesPerSecond;
        long cntAdmin = 0;
        long cntBackp = 0;
        long cnt = 0;
        final long t0 = clock.nanoTime();
        while (cnt < messageCount) {
            long tCur = clock.nanoTime();
            while (tCur - t0 < cnt * periodNs) {
                tCur = clock.nanoTime();
            }
            final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(snapshot.builder(), marketDataDepth, marketDataDepth);
            final int len = encode(unsafeBuffer, newSnapshot);
            long pubres;
            do {
                pubres = publication.offer(unsafeBuffer, 0, len);
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
    }

    private static void awaitConnection(final Publication publication, final long timeout, final TimeUnit unit) {
        if (publication == null) {
            throw new IllegalStateException("not started");
        }
        final long millis = unit.toMillis(timeout);
        long wait = millis;
        while (!publication.isConnected() && wait > 0) {
            try {
                Thread.sleep(Math.min(100, wait));
                wait -= 100;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!publication.isConnected()) {
            throw new RuntimeException("not connected after " + timeout + " " + unit);
        }
    }

}