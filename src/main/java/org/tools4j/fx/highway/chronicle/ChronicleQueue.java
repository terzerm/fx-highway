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
package org.tools4j.fx.highway.chronicle;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.File;
import java.io.IOException;

/**
 * Created by terz on 31/05/2016.
 */
public class ChronicleQueue {

    private Chronicle queue;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;

    public ChronicleQueue() throws IOException {
        final File tmpDir= new File(System.getProperty("java.io.tmpdir"));
        final File basePath = new File(System.getProperty("java.io.tmpdir"), "chronicle");
        for (final File file : tmpDir.listFiles()) {
            if (file.getCanonicalPath().startsWith(basePath.getCanonicalPath())) {
                deleteRecursively(file);
            }
        }

        this.queue = ChronicleQueueBuilder.indexed(basePath.getPath()).build();
        this.appender = queue.createAppender();
        this.tailer = queue.createTailer();
    }

    private static void deleteRecursively(final File file) throws IOException {
        if (file.isDirectory()) {
            for (File sub : file.listFiles()) {
                deleteRecursively(sub);
            }
        }
        if (!file.delete()) {
            throw new IOException("could not delete: " + file.getAbsolutePath());
        }
    }

    public ExcerptAppender getAppender() {
        return appender;
    }

    public ExcerptTailer getTailer() {
        return tailer;
    }

    public void close() throws IOException {
        appender = null;
        tailer = null;
        queue.close();
        queue = null;
    }
}
