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

/**
 * Message writer offers methods to write different value types for elements of a message.
 */
public interface MessageWriter {
    MessageWriter putBoolean(boolean value);
    MessageWriter putInt8(byte value);
    MessageWriter putInt8(int value);
    MessageWriter putInt16(short value);
    MessageWriter putInt16(int value);
    MessageWriter putInt32(int value);
    MessageWriter putInt64(long value);
    MessageWriter putFloat32(float value);
    MessageWriter putFloat64(double value);
    MessageWriter putCharAscii(char value);
    MessageWriter putChar(char value);
    MessageWriter putStringAscii(CharSequence value);
    /**
     * Writes a string using
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
     * encoding in a machine-independent manner.
     * <p>
     * First, two bytes are written to out as if by the <code>writeShort</code>
     * method giving the number of bytes to follow. This value is the number of
     * bytes actually written out, not the length of the string. Following the
     * length, each character of the string is output, in sequence, using the
     * modified UTF-8 encoding for the character. The number of bytes written
     * will be at least two plus the length of <code>value</code>, and at most two
     * plus thrice the length of <code>value</code>.
     *
     * @param      value   a string to be written.
     * @return     This message writer for chained put operations
     */
    MessageWriter putStringUtf8(CharSequence value);
    MessageWriter putString(CharSequence value);
    Appender finishAppendMessage();
}
