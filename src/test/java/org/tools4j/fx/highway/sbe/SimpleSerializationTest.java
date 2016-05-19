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

import com.google.common.collect.Lists;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.RateLevel;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimpleSerializationTest {

    private static final MessageHeaderDecoder MESSAGE_HEADER_DECODER = new MessageHeaderDecoder();
    private static final MessageHeaderEncoder MESSAGE_HEADER_ENCODER = new MessageHeaderEncoder();
    private static final MarketDataSnapshotDecoder MD_SNAPSHOT_DECODER = new MarketDataSnapshotDecoder();
    private static final MarketDataSnapshotEncoder MD_SNAPSHOT_ENCODER = new MarketDataSnapshotEncoder();


    @Test
    public void shouldSerializeToSBEAndDeserializeFromSBETheSameMarketDataSnapshot() throws Exception {

        final long triggerTimestamp = Instant.now().toEpochMilli();
        final long eventTimestamp = Instant.now().minusSeconds(10).toEpochMilli();
        final CurrencyPair currencyPair = CurrencyPair.AUDUSD;
        final Venue venue = Venue.EBS;
        final double bidQuantity1 = 1000000;
        final double bidRate1 = 0.7524;
        final double bidQuantity2 = 2000000;
        final double bidRate2 = 0.7522;

        final double askQuantity1 = 1000000;
        final double askRate1 = 0.7534;
        final double askQuantity2 = 2000000;
        final double askRate2 = 0.7537;


        MarketDataSnapshot newSnapshot = new MarketDataSnapshot(triggerTimestamp, eventTimestamp, currencyPair, venue,
                Lists.newArrayList(
                        new RateLevel(bidQuantity1, bidRate1),
                        new RateLevel(bidQuantity2, bidRate2)),
                Lists.newArrayList(
                        new RateLevel(askQuantity1, askRate1),
                        new RateLevel(askQuantity2, askRate2)));


        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
        int bufferOffset = 0;
        int encodingLength = 0;

        MESSAGE_HEADER_ENCODER
                .wrap(directBuffer, bufferOffset)
                .blockLength(MD_SNAPSHOT_ENCODER.sbeBlockLength())
                .templateId(MD_SNAPSHOT_ENCODER.sbeTemplateId())
                .schemaId(MD_SNAPSHOT_ENCODER.sbeSchemaId())
                .version(MD_SNAPSHOT_ENCODER.sbeSchemaVersion());

        bufferOffset += MESSAGE_HEADER_ENCODER.encodedLength();
        encodingLength += MESSAGE_HEADER_ENCODER.encodedLength();
        encodingLength += encode(MD_SNAPSHOT_ENCODER, directBuffer, bufferOffset, newSnapshot);


        // Decode the encoded message

        bufferOffset = 0;
        MESSAGE_HEADER_DECODER.wrap(directBuffer, bufferOffset);

        // Lookup the applicable flyweight to decode this type of message based on templateId and version.
        final int templateId = MESSAGE_HEADER_DECODER.templateId();
        if (templateId != MarketDataSnapshotEncoder.TEMPLATE_ID)
        {
            throw new IllegalStateException("Template ids do not match");
        }

        final int actingBlockLength = MESSAGE_HEADER_DECODER.blockLength();
        final int schemaId = MESSAGE_HEADER_DECODER.schemaId(); //I don't use it yet
        final int actingVersion = MESSAGE_HEADER_DECODER.version();

        bufferOffset += MESSAGE_HEADER_DECODER.encodedLength();
        MarketDataSnapshot decodedSnapshot = decode(MD_SNAPSHOT_DECODER, directBuffer, bufferOffset, actingBlockLength,  actingVersion);

        assertThat(decodedSnapshot, is(newSnapshot));
     }


    public static int encode(final MarketDataSnapshotEncoder mdSnapshot, final UnsafeBuffer directBuffer, final int bufferOffset, final MarketDataSnapshot fromSnapshot) {
        final int srcOffset = 0;

        mdSnapshot.wrap(directBuffer, bufferOffset)
                .triggerTimestamp(fromSnapshot.getTriggerTimestamp())
                .eventTimestamp(fromSnapshot.getEventTimestamp())
                .currencyPair(fromSnapshot.getCurrencyPair())
                .venue(fromSnapshot.getVenue());

        final MarketDataSnapshotEncoder.BidsEncoder bidsEncoder = mdSnapshot.bidsCount(fromSnapshot.getBids().size());

        fromSnapshot.getBids().forEach(b -> bidsEncoder.next().quantity(b.getQuantity()).rate(b.getRate()));

        final MarketDataSnapshotEncoder.AsksEncoder asksEncoder = mdSnapshot.asksCount(fromSnapshot.getAsks().size());

        fromSnapshot.getAsks().forEach(a -> asksEncoder.next().quantity(a.getQuantity()).rate(a.getRate()));


        return mdSnapshot.encodedLength();
    }


    public static MarketDataSnapshot decode(
            final MarketDataSnapshotDecoder mdSnapshotDecoder,
            final UnsafeBuffer directBuffer,
            final int bufferOffset,
            final int actingBlockLength,
            final int actingVersion)
            throws Exception
    {

        final long triggerTimestamp;
        final long eventTimestamp;
        final CurrencyPair currencyPair;
        final Venue venue;


        mdSnapshotDecoder.wrap(directBuffer, bufferOffset, actingBlockLength, actingVersion);

        //mdSnapshotDecoder.sbeTemplateId();
        //mdSnapshotDecoder. schemaId
        //mdSnapshotDecoder.sbeSchemaVersion();

        triggerTimestamp = mdSnapshotDecoder.triggerTimestamp();
        eventTimestamp = mdSnapshotDecoder.eventTimestamp();
        currencyPair = mdSnapshotDecoder.currencyPair();
        venue = mdSnapshotDecoder.venue();

        List<RateLevel> bids = Lists.newArrayList();
        List<RateLevel> asks = Lists.newArrayList();

        mdSnapshotDecoder.bids().forEach(bd -> bids.add(new RateLevel(bd.quantity(), bd.rate())));
        mdSnapshotDecoder.asks().forEach(ad -> asks.add(new RateLevel(ad.quantity(), ad.rate())));

        return new MarketDataSnapshot(triggerTimestamp, eventTimestamp, currencyPair, venue, bids, asks);
    }

}
