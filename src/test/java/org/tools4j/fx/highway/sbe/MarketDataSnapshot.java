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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class MarketDataSnapshot {
    private final long triggerTimestamp;
    private final long eventTimestamp;
    private final CurrencyPair currencyPair;
    private final Venue venue;
    private final List<RateLevel> bids;
    private final List<RateLevel> asks;

    public MarketDataSnapshot(long triggerTimestamp, long eventTimestamp, CurrencyPair currencyPair, Venue venue, Iterable<RateLevel> bids, Iterable<RateLevel> asks) {
        this.triggerTimestamp = triggerTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.currencyPair = Objects.requireNonNull(currencyPair);
        this.venue = Objects.requireNonNull(venue);
        this.bids = ImmutableList.<RateLevel>builder().addAll(bids).build();
        this.asks = ImmutableList.<RateLevel>builder().addAll(asks).build();

    }

    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public CurrencyPair getCurrencyPair() {
        return currencyPair;
    }

    public Venue getVenue() {
        return venue;
    }

    public List<RateLevel> getBids() {
        return bids;
    }

    public List<RateLevel> getAsks() {
        return asks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MarketDataSnapshot that = (MarketDataSnapshot) o;
        return triggerTimestamp == that.triggerTimestamp &&
                eventTimestamp == that.eventTimestamp &&
                Objects.equals(currencyPair, that.currencyPair) &&
                Objects.equals(venue, that.venue) &&
                Objects.equals(bids, that.bids) &&
                Objects.equals(asks, that.asks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(triggerTimestamp, eventTimestamp, currencyPair, venue, bids, asks);
    }
}
