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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tools4j.fx.highway.util.AeronStandaloneLatencyTestRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Starts 3 VM's, one for each of aeron media driver, publisher and subscriber.
 */
@RunWith(Parameterized.class)
public class AeronStandaloneLatencyTest {

    private final String channel;
    private final long messagesPerSecond;
    private final int marketDataDepth;

    private AeronStandaloneLatencyTestRunner latencyTestRunner;

    @Parameterized.Parameters(name = "{index}: CH={0}, MPS={1}, D={2}")
    public static Collection testRunParameters() {
        return Arrays.asList(new Object[][] {
                { "aeron:ipc", 160000, 2 },
//                { "aeron:ipc", 320000, 2 },
//                { "aeron:ipc", 500000, 2 },
//                { "udp://localhost:40123", 160000, 2 },
                { "udp://224.10.9.7:4050", 160000, 2 }
        });
    }

    public AeronStandaloneLatencyTest(final String channel,
                                      final long messagesPerSecond,
                                      final int marketDataDepth) {
        this.channel = Objects.requireNonNull(channel);
        this.messagesPerSecond = messagesPerSecond;
        this.marketDataDepth = marketDataDepth;
    }

    @Test
    public void latencyTest() throws Exception {
        latencyTestRunner = new AeronStandaloneLatencyTestRunner(channel, 10, messagesPerSecond, 200000, 100000, marketDataDepth);
        latencyTestRunner.start();
        try {
            Thread.sleep(5000);//give some time for media driver to become ready
            latencyTestRunner.runLatencyTest();
            latencyTestRunner.waitFor(30, TimeUnit.SECONDS);
        } finally {
            latencyTestRunner.shutdown();
        }
    }
}
