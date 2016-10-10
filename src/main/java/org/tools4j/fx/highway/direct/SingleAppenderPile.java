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
package org.tools4j.fx.highway.direct;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.tools4j.fx.highway.direct.DirectUnsafe.UNSAFE;

/**
 * Pile implementation optimised for single Appender and multiple Sequencer support.
 */
public class SingleAppenderPile implements Pile {

    public static final long DEFAULT_REGION_SIZE = 4L << 20;//4 MB

    private final MappedFile file;
    private final AtomicBoolean appenderCreated = new AtomicBoolean(false);

    public SingleAppenderPile(final MappedFile file) {
        this.file = Objects.requireNonNull(file);
    }

    public static final Pile create(final String fileName) throws IOException {
        return create(fileName, DEFAULT_REGION_SIZE);
    }

    public static final Pile create(final String fileName, final long regionSize) throws IOException {
        return open(new MappedFile(fileName, MappedFile.Mode.READ_WRITE_CLEAR, regionSize));
    }

    public static final Pile openForAppending(final String fileName) throws IOException {
        return openForAppending(fileName, DEFAULT_REGION_SIZE);
    }

    public static final Pile openForAppending(final String fileName, final long regionSize) throws IOException {
        return open(new MappedFile(fileName, MappedFile.Mode.READ_WRITE, regionSize));
    }

    public static final Pile openReadOnly(final String fileName) throws IOException {
        return openReadOnly(fileName, DEFAULT_REGION_SIZE);
    }

    public static final Pile openReadOnly(final String fileName, final long regionSize) throws IOException {
        return open(new MappedFile(fileName, MappedFile.Mode.READ_ONLY, regionSize));
    }

    public static final Pile open(final MappedFile file) {
        return new SingleAppenderPile(file);
    }

    @Override
    public Appender appender() {
        if (file.getMode() == MappedFile.Mode.READ_ONLY) {
            throw new IllegalStateException("Cannot access appender for file in read-only mode");
        }
        if (appenderCreated.compareAndSet(false, true)) {
            return new AppenderImpl();
        }
        throw new IllegalStateException("Only one appender supported");
    }

    @Override
    public Sequencer sequencer() {
        return new SequencerImpl();
    }

    private final class SequencerImpl implements Sequencer {

        private final MessageReaderImpl messageReader = new MessageReaderImpl();
        private long messageLen = -1;

        @Override
        public boolean hasNextMessage() {
            return getMessageLength() >= 0;
        }

        @Override
        public MessageReader readNextMessage() {
            final long messageLength = getMessageLength();
            if (messageLength >= 0) {
                return messageReader.readNextMessage(messageLength);
            }
            throw new IllegalStateException("No next message found");
        }

        @Override
        public Sequencer skipNextMessage() {
            return readNextMessage().finishReadMessage();
        }

        private long getMessageLength() {
            if (messageLen < 0) {
                messageLen = messageReader.readNextMessageLength();
            }
            return messageLen;
        }

        private final class MessageReaderImpl extends AbstractUnsafeMessageReader {

            private MappedRegion region = file.reserveRegion(0);
            private long offset = 0;
            private long positionEnd = -1;

            private long readNextMessageLength() {
                return UNSAFE.getLongVolatile(null, offset);
            }

            private MessageReader readNextMessage(final long messageLen) {
                if (positionEnd >= 0) {
                    finishReadMessage();
                }
                positionEnd = region.getPosition() + messageLen;
                offset += 8;//skip message length field
                return this;
            }

            @Override
            public Sequencer finishReadMessage() {
                while (positionEnd < region.getPosition() + region.getSize()) {
                    file.releaseRegion(region);
                    region = file.reserveRegion(region.getIndex() + 1);
                }
                offset = positionEnd - region.getPosition();
                return SequencerImpl.this;
            }

            @Override
            protected long getAndIncrementAddress(final int add) {
                if (positionEnd < 0) {
                    throw new IllegalStateException("No current message");
                }
                if (region.getPosition() + add > positionEnd) {
                    throw new IllegalStateException("Attempt to read beyond message length: " + (region.getPosition() + add) + " > " + positionEnd);
                }
                final long off = offset;
                final long newOffset = off + add;
                final long regionSize = region.getSize();
                if (newOffset < regionSize) {
                    offset = newOffset;
                    return region.getAddress(off);
                }
                //roll region
                file.releaseRegion(region);
                region = file.reserveRegion(region.getIndex() + 1);
                offset = add;
                return region.getAddress();
            }
        }
    }

    private final class AppenderImpl implements Appender {

        private final MessageWriterImpl messageWriter;

        public AppenderImpl() {
            MappedRegion region = file.reserveRegion(0);
            long offset = 0;
            long messageLen;
            while ((messageLen = UNSAFE.getLongVolatile(null, offset)) >= 0) {
                offset += 8 + messageLen;
                while (offset > region.getSize()) {
                    file.releaseRegion(region);
                    offset -= region.getSize();
                    region = file.reserveRegion(region.getIndex() + 1);
                }
            }
            this.messageWriter = new MessageWriterImpl(region, offset);
        }

        @Override
        public MessageWriter appendMessage() {
            return messageWriter.startAppendMessage();
        }

        private final class MessageWriterImpl extends AbstractUnsafeMessageWriter {

            private MappedRegion region;
            private long offset;
            private MappedRegion startRegion;
            private long startOffset;
            private long length;

            public MessageWriterImpl(final MappedRegion region, final long offset) {
                this.region = Objects.requireNonNull(region);
                this.offset = offset;
            }

            private MessageWriter startAppendMessage() {
                if (startRegion != null) {
                    throw new IllegalStateException("Current message is not finished, must be finished before appending next");
                }
                startRegion = region;
                startOffset = offset;
                getAndIncrementAddress(8);//skip length field of new message
                return this;
            }

            @Override
            protected long getAndIncrementAddress(final int add) {
                if (startRegion == null) {
                    throw new IllegalStateException("Message already finished");
                }
                final long off = offset;
                final long newOffset = off + add;
                final long regionSize = region.getSize();
                if (newOffset < regionSize) {
                    offset = newOffset;
                    return region.getAddress(off);
                }
                //roll region
                pad(regionSize - off);
                if (region != startRegion) {
                    file.releaseRegion(region);
                }
                region = file.reserveRegion(region.getIndex() + 1);
                offset = add;
                return region.getAddress();
            }

            @Override
            public Appender finishAppendMessage() {
                if (startRegion == null) {
                    throw new IllegalStateException("No message to finish");
                }
                padMessageEnd();
                writeNextMessageLength();
                writeMessageLength();
                return AppenderImpl.this;
            }

            private void writeMessageLength() {
                UNSAFE.putLongVolatile(null, startRegion.getAddress(startOffset), length);
                if (startRegion != region) {
                    file.releaseRegion(startRegion);
                }
                startRegion = null;
                startOffset = -1;
                length = -1;
            }

            private void writeNextMessageLength() {
                UNSAFE.putLongVolatile(null, getAndIncrementAddress(8), -1);
            }

            private void padMessageEnd() {
                int pad = 8 - (int) (offset & (MappedFile.REGION_SIZE_MULTIPLE - 1));
                if (pad < 8) {
                    pad(pad);
                }
            }
            private void pad(final long n) {
                long c = n;
                while (c >= 8) {
                    putInt64(0);
                    c -= 8;
                }
                if (c >= 4) {
                    putInt32(0);
                    c -= 4;
                }
                if (c >= 2) {
                    putInt16(0);
                    c -= 2;
                }
                if (c >= 1) {
                    putInt8(0);
                }
            }
        }
    }
}
