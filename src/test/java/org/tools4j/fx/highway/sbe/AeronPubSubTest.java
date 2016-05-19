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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tools4j.fx.highway.message.MarketDataSnapshot;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
        final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot();

        //when
        encode(directBuffer, newSnapshot);
        final MarketDataSnapshot decodedSnapshot = decode(directBuffer);

        //then
        assertThat(decodedSnapshot, is(newSnapshot));
     }

}
