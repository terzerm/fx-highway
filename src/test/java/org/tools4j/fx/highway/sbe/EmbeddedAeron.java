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
import io.aeron.driver.ThreadingMode;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Starts an embedded aeron instance and adds a publisher and a subscriber.
 */
public class EmbeddedAeron {

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription subscription;
    private final Publication publication;

    public EmbeddedAeron() {
        this("udp://localhost:40123", 10);
    }
    public EmbeddedAeron(final String channel, final int streamId) {
        final MediaDriver.Context mctx = new MediaDriver.Context();
        mctx.threadingMode(ThreadingMode.DEDICATED);
        mediaDriver = MediaDriver.launchEmbedded(mctx);
        final Aeron.Context actx = new Aeron.Context();
        actx.aeronDirectoryName(mediaDriver.aeronDirectoryName());
        aeron = Aeron.connect(actx);
        subscription = aeron.addSubscription(channel, streamId);
        publication = aeron.addPublication(channel, streamId);
    }

    public Publication getPublication() {
        return publication;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public boolean awaitConnection(final long timeout, final TimeUnit unit) {
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
        return publication.isConnected();
    }

    public void shutdown() {
        closeResume(() -> publication.close());
        closeResume(() -> subscription.close());
        closeResume(() -> aeron.close());
        closeResume(() -> mediaDriver.close());
    }

    private void closeResume(final Closeable c) {
        try {
            c.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
