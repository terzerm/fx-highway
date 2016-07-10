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
package org.tools4j.fx.highway.mappedbus;

import io.mappedbus.MappedBusReader;
import io.mappedbus.MappedBusWriter;
import org.agrona.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;

public class MappedBusFile {

    private final MappedBusReader reader;
    private final MappedBusWriter writer;
    private final byte[] readBuffer;
    private final byte[] writeBuffer;

    public MappedBusFile(final String fileName, final long fileSize, final int recordSize) throws IOException {
        this.reader = new MappedBusReader(fileName, fileSize, recordSize);
        this.writer = new MappedBusWriter(fileName, fileSize, recordSize, false);
        this.readBuffer = new byte[recordSize];
        this.writeBuffer = new byte[recordSize];
        this.writer.open();
        this.reader.open();
    }

    public void write(final DirectBuffer buffer, final int offset, final int len) {
        if (len > 0) {
            buffer.getBytes(offset, writeBuffer, 0, len);
            try {
                writer.write(writeBuffer, 0, len);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int poll(final DirectBuffer buffer) {
        try {
            if (reader.next()) {
                final int len = reader.readBuffer(readBuffer, 0);
                if (len > 0) {
                    buffer.wrap(readBuffer, 0, len);
                    return len;
                }
            }
            return 0;
        } catch (Exception e) {
            return -1;
        }
    }

    public void close() {
        closeResume(() -> reader.close());
        closeResume(() -> writer.close());
    }

    private void closeResume(final Closeable c) {
        try {
            c.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
