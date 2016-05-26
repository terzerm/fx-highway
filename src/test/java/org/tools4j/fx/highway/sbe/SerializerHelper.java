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

import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.tools4j.fx.highway.message.MarketDataSnapshot;
import org.tools4j.fx.highway.message.MarketDataSnapshotBuilder;
import org.tools4j.fx.highway.message.RateLevel;

public class SerializerHelper {

    public static final NanoClock NANO_CLOCK = new SystemNanoClock();
    private static final MessageHeaderDecoder MESSAGE_HEADER_DECODER = new MessageHeaderDecoder();
    private static final MessageHeaderEncoder MESSAGE_HEADER_ENCODER = new MessageHeaderEncoder();
    private static final MarketDataSnapshotDecoder MD_SNAPSHOT_DECODER = new MarketDataSnapshotDecoder();
    private static final MarketDataSnapshotEncoder MD_SNAPSHOT_ENCODER = new MarketDataSnapshotEncoder();

    public static MarketDataSnapshot givenMarketDataSnapshot(final MarketDataSnapshotBuilder builder) {
        return givenMarketDataSnapshot(builder, 10, 10);
    }

    public static MarketDataSnapshot givenMarketDataSnapshot(final MarketDataSnapshotBuilder builder,
                                                      final int bids, final int asks) {
        final long triggerTimestamp = NANO_CLOCK.nanoTime();
        final long eventTimestamp = triggerTimestamp;
        final CurrencyPair currencyPair = CurrencyPair.AUDUSD;
        final Venue venue = Venue.EBS;
        final double bidQuantity = 1000000;
        final double bidRate = 0.7524;
        final double bidDiff = 0.0001;

        final double askQuantity = 1000000;
        final double askRate = 0.7534;
        final double askDiff = 0.0001;

        builder.setTriggerTimestamp(triggerTimestamp);
        builder.setEventTimestamp(eventTimestamp);
        builder.setCurrencyPair(currencyPair);
        builder.setVenue(venue);
        for (int i = 0; i < bids; i++) {
            builder.addBid(i*bidQuantity, bidRate - i*bidDiff);
        }
        for (int i = 0; i < asks; i++) {
            builder.addAsk(i*askQuantity, askRate + i*askDiff);
        }

        return builder.build();
    }

    public static int encode(final UnsafeBuffer directBuffer,
                      final MarketDataSnapshot fromSnapshot) {
        MESSAGE_HEADER_ENCODER
                .wrap(directBuffer, 0)
                .blockLength(MD_SNAPSHOT_ENCODER.sbeBlockLength())
                .templateId(MD_SNAPSHOT_ENCODER.sbeTemplateId())
                .schemaId(MD_SNAPSHOT_ENCODER.sbeSchemaId())
                .version(MD_SNAPSHOT_ENCODER.sbeSchemaVersion());

        MD_SNAPSHOT_ENCODER.wrap(directBuffer, MESSAGE_HEADER_ENCODER.encodedLength())
                .triggerTimestamp(fromSnapshot.getTriggerTimestamp())
                .eventTimestamp(fromSnapshot.getEventTimestamp())
                .currencyPair(fromSnapshot.getCurrencyPair())
                .venue(fromSnapshot.getVenue());

        final MarketDataSnapshotEncoder.BidsEncoder bidsEncoder = MD_SNAPSHOT_ENCODER.bidsCount(fromSnapshot.getBids().size());
        for (int i = 0; i < fromSnapshot.getBids().size(); i++) {
            final RateLevel l = fromSnapshot.getBids().get(i);
            bidsEncoder.next().quantity(l.getQuantity()).rate(l.getRate());
        }

        final MarketDataSnapshotEncoder.AsksEncoder asksEncoder = MD_SNAPSHOT_ENCODER.asksCount(fromSnapshot.getAsks().size());
        for (int i = 0; i < fromSnapshot.getAsks().size(); i++) {
            final RateLevel l = fromSnapshot.getAsks().get(i);
            asksEncoder.next().quantity(l.getQuantity()).rate(l.getRate());
        }

        return MESSAGE_HEADER_ENCODER.encodedLength() + MD_SNAPSHOT_ENCODER.encodedLength();
    }


    public static MarketDataSnapshot decode(final UnsafeBuffer directBuffer, final MarketDataSnapshotBuilder builder) {
        MESSAGE_HEADER_DECODER.wrap(directBuffer, 0);

        // Lookup the applicable flyweight to decode this type of message based on templateId and version.
        final int templateId = MESSAGE_HEADER_DECODER.templateId();
        if (templateId != MarketDataSnapshotEncoder.TEMPLATE_ID)
        {
            throw new IllegalStateException("Template ids do not match");
        }

        final int actingBlockLength = MESSAGE_HEADER_DECODER.blockLength();
        final int schemaId = MESSAGE_HEADER_DECODER.schemaId(); //I don't use it yet
        final int actingVersion = MESSAGE_HEADER_DECODER.version();

        MD_SNAPSHOT_DECODER.wrap(directBuffer, MESSAGE_HEADER_DECODER.encodedLength(), actingBlockLength, actingVersion);

        //mdSnapshotDecoder.sbeTemplateId();
        //mdSnapshotDecoder. schemaId
        //mdSnapshotDecoder.sbeSchemaVersion();

        builder.setTriggerTimestamp(MD_SNAPSHOT_DECODER.triggerTimestamp());
        builder.setEventTimestamp(MD_SNAPSHOT_DECODER.eventTimestamp());
        builder.setCurrencyPair(MD_SNAPSHOT_DECODER.currencyPair());
        builder.setVenue(MD_SNAPSHOT_DECODER.venue());

        final MarketDataSnapshotDecoder.BidsDecoder bidsDecoder = MD_SNAPSHOT_DECODER.bids();
        while (bidsDecoder.hasNext()) {
            bidsDecoder.next();
            builder.addBid(bidsDecoder.quantity(), bidsDecoder.rate());
        }
        final MarketDataSnapshotDecoder.AsksDecoder asksDecoder = MD_SNAPSHOT_DECODER.asks();
        while (asksDecoder.hasNext()) {
            asksDecoder.next();
            builder.addAsk(asksDecoder.quantity(), asksDecoder.rate());
        }

        return builder.build();
    }

}
