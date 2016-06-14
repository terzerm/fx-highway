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

import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireType;
import org.tools4j.fx.highway.util.FileUtil;

import java.io.File;
import java.io.IOException;

public class ChronicleQueue4x {

    private net.openhft.chronicle.queue.ChronicleQueue queue;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;

    public ChronicleQueue4x() throws IOException {
        FileUtil.deleteTmpDirFilesMatching("chronicle-queue4x");
        final File basePath = FileUtil.tmpDirFile("chronicle-queue4x");

        this.queue = ChronicleQueueBuilder
                .single(basePath.getPath())
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(128 << 20)
                .build();
        this.appender = queue.createAppender();
        this.tailer = queue.createTailer();
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
