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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tools4j.fx.highway.message.ImmutableMarketDataSnapshot;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.tools4j.fx.highway.sbe.SerializerHelper.encode;
import static org.tools4j.fx.highway.sbe.SerializerHelper.givenMarketDataSnapshot;

/**
 * Starts 3 VM's, one for each of aeron media driver, publisher and subscriber.
 */
@RunWith(Parameterized.class)
public class AeronStandaloneLatencyTest {

    private final String channel;
    private final long messagesPerSecond;
    private final int marketDataDepth;

    private AeronMediaDriver aeronMediaDriver;
    private AeronSubscriber aeronSubscriber;
    private AeronPublisher aeronPublisher;

    @Parameterized.Parameters(name = "{index}: CH={0}, MPS={1}, D={2}")
    public static Collection testRunParameters() {
        return Arrays.asList(new Object[][] {
                { "aeron:ipc", 160000, 2 },
//                { "aeron:ipc", 500000, 2 },
//                { "udp://localhost:40123", 160000, 2 },
//                { "udp://224.10.9.7:4050", 160000, 2 }
        });
    }

    public AeronStandaloneLatencyTest(final String channel,
                                      final long messagesPerSecond,
                                      final int marketDataDepth) {
        this.channel = Objects.requireNonNull(channel);
        this.messagesPerSecond = messagesPerSecond;
        this.marketDataDepth = marketDataDepth;
    }

    @Before
    public void setup() throws InterruptedException {
        aeronMediaDriver = new AeronMediaDriver();
        aeronMediaDriver.start();
        Thread.sleep(2000);
    }

    @After
    public void tearDown() {
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

    @Test
    public void latencyTest() throws Exception {
        //given
        final int streamId = 10;
        final long warmupCount = 1000000;
        final long measureCount = 200000;
        final int marketDataDepth = 2;
        final long n = warmupCount + measureCount;

        System.out.println("\twarmup + measured : " + warmupCount + " + " + measureCount + " = " + n);
        System.out.println("\tchannel           : " + channel);
        System.out.println("\tstreamId          : " + streamId);
        System.out.println("\tmessagesPerSecond : " + messagesPerSecond);
        System.out.println("\tmarketDataDepth   : " + marketDataDepth);
        System.out.println("\tmessageSize       : " + encode(new UnsafeBuffer(new byte[1024]), givenMarketDataSnapshot(new ImmutableMarketDataSnapshot.Builder(), marketDataDepth, marketDataDepth)) + " bytes");
        System.out.println();

        //when
        aeronSubscriber = new AeronSubscriber(aeronMediaDriver.getAeronDirectoryName(), channel, streamId, warmupCount, measureCount);
        aeronSubscriber.start();
        aeronPublisher = new AeronPublisher(aeronMediaDriver.getAeronDirectoryName(), channel, streamId, n, messagesPerSecond, marketDataDepth);
        aeronPublisher.start();

        //then
        aeronSubscriber.waitFor(30, TimeUnit.SECONDS);
    }
}
