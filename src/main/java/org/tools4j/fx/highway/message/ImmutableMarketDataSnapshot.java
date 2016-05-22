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
package org.tools4j.fx.highway.message;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.tools4j.fx.highway.sbe.CurrencyPair;
import org.tools4j.fx.highway.sbe.Venue;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ImmutableMarketDataSnapshot implements MarketDataSnapshot {
    private final long triggerTimestamp;
    private final long eventTimestamp;
    private final CurrencyPair currencyPair;
    private final Venue venue;
    private final List<ImmutableRateLevel> bids;
    private final List<ImmutableRateLevel> asks;

    public static final class Builder implements MarketDataSnapshotBuilder {

        private long triggerTimestamp;
        private long eventTimestamp;
        private CurrencyPair currencyPair;
        private Venue venue;
        private final List<ImmutableRateLevel> bids = Lists.newArrayList();
        private final List<ImmutableRateLevel> asks = Lists.newArrayList();

        @Override
        public void setTriggerTimestamp(long triggerTimestamp) {
            this.triggerTimestamp = triggerTimestamp;
        }

        @Override
        public void setEventTimestamp(long eventTimestamp) {
            this.eventTimestamp = eventTimestamp;
        }

        @Override
        public void setCurrencyPair(CurrencyPair currencyPair) {
            this.currencyPair = currencyPair;
        }

        @Override
        public void setVenue(Venue venue) {
            this.venue = venue;
        }

        @Override
        public void addBid(double quantity, double rate) {
            bids.add(new ImmutableRateLevel(quantity, rate));
        }

        @Override
        public void addAsk(double quantity, double rate) {
            asks.add(new ImmutableRateLevel(quantity, rate));
        }

        @Override
        public MarketDataSnapshot build() {
            return new ImmutableMarketDataSnapshot(triggerTimestamp, eventTimestamp, currencyPair, venue, bids, asks);
        }
    }

    public ImmutableMarketDataSnapshot(long triggerTimestamp, long eventTimestamp, CurrencyPair currencyPair, Venue venue, Iterable<ImmutableRateLevel> bids, Iterable<ImmutableRateLevel> asks) {
        this.triggerTimestamp = triggerTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.currencyPair = currencyPair;
        this.venue = venue;
        this.bids = ImmutableList.<ImmutableRateLevel>builder().addAll(bids == null ? Collections.emptyList() : bids).build();
        this.asks = ImmutableList.<ImmutableRateLevel>builder().addAll(asks == null ? Collections.emptyList() : asks).build();

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

    public List<ImmutableRateLevel> getBids() {
        return bids;
    }

    public List<ImmutableRateLevel> getAsks() {
        return asks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ImmutableMarketDataSnapshot that = (ImmutableMarketDataSnapshot) o;
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

    @Override
    public String toString() {
        return "ImmutableMarketDataSnapshot{" +
                "triggerTimestamp=" + triggerTimestamp +
                ", eventTimestamp=" + eventTimestamp +
                ", currencyPair=" + currencyPair +
                ", venue=" + venue +
                ", bids=" + bids +
                ", asks=" + asks +
                '}';
    }
}
