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
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.MutableMarketDataSnapshot;
import org.tools4j.fx.highway.util.SerializerHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Starts the aeron publisher in a separate process.
 */
public class AeronSubscriber extends AbstractAeronProcess {

    private final String channel;
    private final int streamId;
    private final long warmupCount;
    private final long measuredCount;

    public AeronSubscriber(final String aeronDirectoryName,
                           final String channel, final int streamId,
                           final long warmupCount, final long measuredCount) {
        super(aeronDirectoryName);
        this.channel = Objects.requireNonNull(channel);
        this.streamId = streamId;
        this.warmupCount = warmupCount;
        this.measuredCount = measuredCount;
    }

    public String getChannel() {
        return channel;
    }

    public int getStreamId() {
        return streamId;
    }

    public long getWarmupCount() {
        return warmupCount;
    }

    public long getMeasuredCount() {
        return measuredCount;
    }

    public void start() {
        super.start(AeronSubscriber.class);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    protected List<String> mainArgs() {
        final List<String> args = new ArrayList<>();
        args.add(getAeronDirectoryName());
        args.add(getChannel());
        args.add(String.valueOf(getStreamId()));
        args.add(String.valueOf(getWarmupCount()));
        args.add(String.valueOf(getMeasuredCount()));
        return args;
    }

    public static void main(final String... args) {
        final String aeronDirectoryName = args[0];
        final String channel = args[1];
        final int streamId = Integer.parseInt(args[2]);
        final long warmupCount = Long.parseLong(args[3]);
        final long measuredCount = Long.parseLong(args[4]);

        System.out.println("Started " + AeronSubscriber.class.getSimpleName() + ":");
        System.out.println("\twarmupCount       : " + warmupCount);
        System.out.println("\tmeasuredCount     : " + measuredCount);
        System.out.println("\tchannel           : " + channel);
        System.out.println("\tstreamId          : " + streamId);
        System.out.println();

        final Aeron aeron = aeron(aeronDirectoryName);
        final Subscription subscription = aeron.addSubscription(channel, streamId);
        try {
            run(subscription, warmupCount, measuredCount);
        } finally {
            subscription.close();
            aeron.close();
            System.out.println("Shutdown " + AeronSubscriber.class.getSimpleName() + "...");
        }
    }

    private static void run(final Subscription subscription, final long warmupCount, final long measuredCount) {
        final NanoClock clock = new SystemNanoClock();
        final Histogram histogram = new Histogram(1, 1000000000, 3);
        final MutableMarketDataSnapshot snapshot = new MutableMarketDataSnapshot();
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(0, 0);
        final AtomicLong t0 = new AtomicLong();
        final AtomicLong t1 = new AtomicLong();
        final AtomicLong t2 = new AtomicLong();
        final long n = warmupCount + measuredCount;
        final AtomicLong count = new AtomicLong();
        final FragmentHandler fh = (buf, offset, len, header) -> {
            if (count.get() == 0) t0.set(clock.nanoTime());
            else if (count.get() == warmupCount-1) t1.set(clock.nanoTime());
            else if (count.get() == n-1) t2.set(clock.nanoTime());
            unsafeBuffer.wrap(buf, offset, len);
            final MarketDataSnapshot decoded = SerializerHelper.decode(unsafeBuffer, snapshot.builder());
            final long time = clock.nanoTime();
            if (count.incrementAndGet() <= n) {
                histogram.recordValue(time - decoded.getEventTimestamp());
            }
            if (count.get() == warmupCount) {
                histogram.reset();
            }
        };
        while (count.get() < n) {
            subscription.poll(fh, 256);
        }
        final long c = count.get();
        System.out.println((t2.get() - t0.get())/1000.0 + " us total receiving time (" + (t2.get() - t0.get())/(1000f*c) + " us/message, " + c/((t2.get()-t0.get())/1000000000f) + " messages/second)");
        System.out.println();

        printStats(histogram);
    }

    private static void printStats(final Histogram histogram) {
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