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
import java.io.RandomAccessFile;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by terz on 9/10/2016.
 */
public class MappedFile {

    public static final long DEFAULT_REGION_SIZE = 4L<<20;//4MB

    //must be power of 2!
    public static final long REGION_SIZE_MULTIPLE = 8;

    public enum Mode {
        READ_ONLY("r"),
        READ_WRITE("rw"),
        /** Delete file contents on open*/
        READ_WRITE_CLEAR("rw");

        private final String rasMode;
        Mode(final String rasMode) {
            this.rasMode = Objects.requireNonNull(rasMode);
        }

        public String getRandomAccessMode() {
            return rasMode;
        }
    }

    private final RandomAccessFile file;
    private final Mode mode;
    private final long regionSize;

    private volatile AtomicReferenceArray<MappedRegion> mappedRegions = new AtomicReferenceArray<MappedRegion>(2);

    public MappedFile(final String fileName, final Mode mode) throws IOException {
        this(fileName, mode, DEFAULT_REGION_SIZE);
    }

    public MappedFile(final String fileName, final Mode mode, final long regionSize) throws IOException {
        this(new RandomAccessFile(fileName, mode.getRandomAccessMode()), mode, regionSize);
    }

    private MappedFile(final RandomAccessFile file, final Mode mode, final long regionSize) throws IOException {
        if (regionSize <= 0 || (regionSize & (REGION_SIZE_MULTIPLE-1)) != 0) {
            throw new IllegalArgumentException("Region size must be positive and a multiple of " + REGION_SIZE_MULTIPLE + " but was " + regionSize);
        }
        this.file = Objects.requireNonNull(file);
        this.mode = Objects.requireNonNull(mode);
        this.regionSize = regionSize;
        if (mode == Mode.READ_WRITE_CLEAR) {
            file.setLength(0);
        }
    }

    public Mode getMode() {
        return mode;
    }

    public long getRegionSize() {
        return regionSize;
    }

    public long getFileLength() {
        try {
            return file.length();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setFileLength(final long length) {
        try {
            file.setLength(length);
        } catch (final IOException e) {
            throw new RuntimeException("could not set file length to " + length, e);
        }
    }

    public int getRegionIndexForPosition(final long position) {
        final long index = position / regionSize;
        return index <= Integer.MAX_VALUE ? (int)index : -1;
    }

    public void releaseRegion(final MappedRegion mappedRegion) {
        if (0 == mappedRegion.decAndGetRefCount()) {
            final long index = mappedRegion.getPosition() / regionSize;
            if (index < mappedRegions.length()) {
                final int ix = (int) index;
                final MappedRegion mr = mappedRegions.get(ix);
                if (mr != null && mr.isClosed()) {
                    mappedRegions.compareAndSet(ix, mr, null);
                }
            }
        }
    }

    public MappedRegion reserveRegion(final int index) {
        ensureSufficientMappedRegionsCapacity(index);
        //ASSERT: index < mappedRegions.length
        final int ix = (int)index;
        final MappedRegion mr = mappedRegions.get(ix);
        if (mr == null || mr.isClosed() || mr.incAndGetRefCount() == 0) {
            final long position = index * regionSize;
            final long fileLen = getFileLength();
            final long newFileLen = Math.max(fileLen, position + regionSize);
            if (newFileLen > fileLen) {
                setFileLength(newFileLen);
            }
            MappedRegion newRegion = new MappedRegion(file.getChannel(), index, position, regionSize, Math.min(fileLen - position, regionSize));
            if (mappedRegions.compareAndSet(ix, mr, newRegion)) {
                return newRegion;
            }
            //region has been created by someone else
            newRegion.decAndGetRefCount();
            return reserveRegion(index);
        }
        //region exists and ref count increment was successful
        return mr;
    }

    private void ensureSufficientMappedRegionsCapacity(final long index) {
        if (index < mappedRegions.length()) {
            return;
        }
        if (index > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("index out of bounds: " + index);
        }
        synchronized (this) {
            final AtomicReferenceArray<MappedRegion> oldArr = mappedRegions;
            final int oldLen = mappedRegions.length();
            if (index < oldLen) {
                final int newLen = Math.max((int) index, oldLen * 2);//overflow would be corrected by max
                final AtomicReferenceArray<MappedRegion> newArr = new AtomicReferenceArray<>(newLen);
                for (int i = 0; i < oldLen; i++) {
                    final MappedRegion mr = oldArr.get(i);
                    if (mr != null && !mr.isClosed()) {
                        newArr.set(i, mr);
                    }
                }
                mappedRegions = newArr;
            }
        }
    }

}
