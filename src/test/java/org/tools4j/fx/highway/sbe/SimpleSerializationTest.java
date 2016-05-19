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

import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.tools4j.fx.highway.message.ImmutableMarketDataSnapshot;
import org.tools4j.fx.highway.message.MarketDataSnapshot;

import java.nio.ByteBuffer;

import static org.tools4j.fx.highway.sbe.SerializerHelper.*;

public class SimpleSerializationTest {

    @Test
    public void shouldSerializeToSBEAndDeserializeFromSBETheSameMarketDataSnapshot() throws Exception {
        //given
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
        final MarketDataSnapshot newSnapshot = givenMarketDataSnapshot(new ImmutableMarketDataSnapshot.Builder());

        //when
        encode(directBuffer, newSnapshot);
        final MarketDataSnapshot decoded = decode(directBuffer, new ImmutableMarketDataSnapshot.Builder());

        //then
        Assertions.assertThat(decoded).isEqualTo(newSnapshot);
    }

}
