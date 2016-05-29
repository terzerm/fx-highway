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
package org.tools4j.fx.highway.util;

import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.fx.highway.aeron.AeronMediaDriver;
import org.tools4j.fx.highway.aeron.AeronPublisher;
import org.tools4j.fx.highway.aeron.AeronSubscriber;
import org.tools4j.fx.highway.message.ImmutableMarketDataSnapshot;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.tools4j.fx.highway.util.SerializerHelper.encode;
import static org.tools4j.fx.highway.util.SerializerHelper.givenMarketDataSnapshot;

/**
 * Starts 3 VM's, one for each of aeron media driver, publisher and subscriber.
 */
public class AeronStandaloneLatencyTestRunner {

    private final String channel;
    private final int streamId;
    private final long messagesPerSecond;
    private final long warmupCount;
    private final long measuredCount;
    private final int marketDataDepth;

    private AeronMediaDriver aeronMediaDriver;
    private AeronSubscriber aeronSubscriber;
    private AeronPublisher aeronPublisher;

    public AeronStandaloneLatencyTestRunner(final String channel,
                                            final int streamId,
                                            final long messagesPerSecond,
                                            final long warmupCount,
                                            final long measuredCount,
                                            final int marketDataDepth) {
        this.channel = Objects.requireNonNull(channel);
        this.streamId = streamId;
        this.messagesPerSecond = messagesPerSecond;
        this.warmupCount = warmupCount;
        this.measuredCount = measuredCount;
        this.marketDataDepth = marketDataDepth;
    }

    public void start() throws InterruptedException {
        aeronMediaDriver = new AeronMediaDriver();
        aeronMediaDriver.start();
    }

    public void shutdown() {
        if (aeronPublisher != null) {
            aeronPublisher.shutdown();
            aeronPublisher = null;
        }
        if (aeronSubscriber != null) {
            aeronSubscriber.shutdown();
            aeronSubscriber = null;
        }
        if (aeronMediaDriver != null) {
            aeronMediaDriver.shutdown();
            aeronMediaDriver = null;
        }
    }

    public void runLatencyTest() throws Exception {
        //given
        final long n = warmupCount + measuredCount;

        System.out.println("\twarmup + measured : " + warmupCount + " + " + measuredCount + " = " + n);
        System.out.println("\tchannel           : " + channel);
        System.out.println("\tstreamId          : " + streamId);
        System.out.println("\tmessagesPerSecond : " + messagesPerSecond);
        System.out.println("\tmarketDataDepth   : " + marketDataDepth);
        System.out.println("\tmessageSize       : " + encode(new UnsafeBuffer(new byte[1024]), givenMarketDataSnapshot(new ImmutableMarketDataSnapshot.Builder(), marketDataDepth, marketDataDepth)) + " bytes");
        System.out.println();

        //when
        aeronSubscriber = new AeronSubscriber(aeronMediaDriver.getAeronDirectoryName(), channel, streamId, warmupCount, measuredCount);
        aeronSubscriber.start();
        aeronPublisher = new AeronPublisher(aeronMediaDriver.getAeronDirectoryName(), channel, streamId, n, messagesPerSecond, marketDataDepth);
        aeronPublisher.start();
    }

    public void waitFor(final long timeout, final TimeUnit timeUnit) {
        aeronSubscriber.waitFor(timeout, timeUnit);
    }

    public static void main(final String... args) throws Exception {
        if (args.length != 7) {
            printUsage();
            System.exit(1);
        }
        try {
            final String channel = args[0];
            final int streamId = Integer.parseInt(args[1]);
            final long messagesPerSecond = Long.parseLong(args[2]);
            final long warmupCount = Long.parseLong(args[3]);
            final long measuredCount = Long.parseLong(args[4]);
            final int marketDataDepth = Integer.parseInt(args[5]);
            final long waitTimeSeconds = Long.parseLong(args[6]);
            final int exitVal = startRunStop(channel, streamId, messagesPerSecond, warmupCount, measuredCount, marketDataDepth, waitTimeSeconds);
            System.exit(exitVal);
        } catch (final Exception e) {
            e.printStackTrace();
            System.err.println();
            System.err.println("Parsing input arguments failed, e=" + e);
            System.err.println();
            printUsage();
            System.exit(2);
        }
    }

    private static int startRunStop(final String channel,
                                    final int streamId,
                                    final long messagesPerSecond,
                                    final long warmupCount,
                                    final long measuredCount,
                                    final int marketDataDepth,
                                    final long waitTimeSeconds) {
        try {
            final AeronStandaloneLatencyTestRunner runner = new AeronStandaloneLatencyTestRunner(
                    channel, streamId,
                    messagesPerSecond, warmupCount, measuredCount,
                    marketDataDepth);
            runner.start();
            try {
                Thread.sleep(2000);//give media driver time to start
                runner.runLatencyTest();
                runner.waitFor(waitTimeSeconds, TimeUnit.SECONDS);
            } finally {
                runner.shutdown();
                Thread.sleep(2000);//give media driver time to shutdown
            }
            return 0;
        } catch (final Exception e) {
            e.printStackTrace();
            System.err.println();
            System.err.println("Running latency test failed, e=" + e);
            System.err.println();
            return 3;
        }
    }

    private static void printUsage() {
        System.err.println("Usage: ");
        System.err.println("    java " + AeronStandaloneLatencyTestRunner.class.getName() + " <channel> <streamId> <messagesPerSecond> <warmupCount> <measuredCount> <marketDataDapth> <waitTimeSeconds>");
        System.err.println("Examples: ");
        System.err.println("    java " + AeronStandaloneLatencyTestRunner.class.getName() + " aeron:ipc 10 160000 200000 1000000 2 30");
        System.err.println("    java " + AeronStandaloneLatencyTestRunner.class.getName() + " udp://localhost:40123 10 500000 1000000 1000000 10 30");
        System.err.println("    java " + AeronStandaloneLatencyTestRunner.class.getName() + " udp://224.10.9.7:4050 10 160000 200000 1000000 2 30");
    }
}
