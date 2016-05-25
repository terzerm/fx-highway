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

import org.tools4j.fx.highway.sbe.CurrencyPair;
import org.tools4j.fx.highway.sbe.Venue;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class MutableMarketDataSnapshot implements MarketDataSnapshot {

    public static final SupplierFactory<MarketDataSnapshotBuilder> BUILDER_FACTORY = () -> new Supplier() {

        private final MutableMarketDataSnapshot snapshot = new MutableMarketDataSnapshot();

        @Override
        public MarketDataSnapshotBuilder get() {
            return snapshot.builder();
        }
    };

    private long triggerTimestamp;
    private long eventTimestamp;
    private CurrencyPair currencyPair;
    private Venue venue;
    private int bidCount = 0;
    private int askCount = 0;
    private final List<MutableRateLevel> bidCache = list(10);
    private final List<MutableRateLevel> askCache = list(10);
    private final List<MutableRateLevel> bids = new AbstractList<MutableRateLevel>() {
        @Override
        public MutableRateLevel get(int index) {
            if (index < 0 || index >= bidCount) {
                throw new IndexOutOfBoundsException("index " + index + " must be in [0," + (bidCount-1) + "]");
            }
            return bidCache.get(index);
        };

        @Override
        public int size() {
            return bidCount;
        }
    };
    private final List<MutableRateLevel> asks = new AbstractList<MutableRateLevel>() {
        @Override
        public MutableRateLevel get(int index) {
            if (index < 0 || index >= askCount) {
                throw new IndexOutOfBoundsException("index " + index + " must be in [0," + (askCount-1) + "]");
            }
            return askCache.get(index);
        };

        @Override
        public int size() {
            return askCount;
        }
    };

    private static final List<MutableRateLevel> list(final int size) {
        final List<MutableRateLevel> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new MutableRateLevel());
        }
        return list;
    }

    private final MarketDataSnapshotBuilder builder = new MarketDataSnapshotBuilder() {

        private final MutableMarketDataSnapshot snapshot = MutableMarketDataSnapshot.this;

        @Override
        public void setTriggerTimestamp(long triggerTimestamp) {
            snapshot.setTriggerTimestamp(triggerTimestamp);
        }

        @Override
        public void setEventTimestamp(long eventTimestamp) {
            snapshot.setEventTimestamp(eventTimestamp);
        }

        @Override
        public void setCurrencyPair(CurrencyPair currencyPair) {
            snapshot.setCurrencyPair(currencyPair);
        }

        @Override
        public void setVenue(Venue venue) {
            snapshot.setVenue(venue);
        }

        @Override
        public void addBid(double quantity, double rate) {
            final int index = snapshot.getBidCount();
            snapshot.setBidCount(index + 1);
            final MutableRateLevel level = snapshot.getBids().get(index);
            level.setQuantity(quantity);
            level.setRate(rate);
        }

        @Override
        public void addAsk(double quantity, double rate) {
            final int index = snapshot.getAskCount();
            snapshot.setAskCount(index + 1);
            final MutableRateLevel level = snapshot.getAsks().get(index);
            level.setQuantity(quantity);
            level.setRate(rate);
        }

        @Override
        public MarketDataSnapshot build() {
            return snapshot;
        }
    };

    public MarketDataSnapshotBuilder builder() {
        setBidCount(0);
        setAskCount(0);
        return builder;
    }

    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public void setTriggerTimestamp(long triggerTimestamp) {
        this.triggerTimestamp = triggerTimestamp;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public CurrencyPair getCurrencyPair() {
        return currencyPair;
    }

    public void setCurrencyPair(CurrencyPair currencyPair) {
        this.currencyPair = currencyPair;
    }

    public Venue getVenue() {
        return venue;
    }

    public void setVenue(Venue venue) {
        this.venue = venue;
    }

    public int getBidCount() {
        return bidCount;
    }

    public void setBidCount(int bidCount) {
        if (bidCount < 0 || bidCount > bidCache.size()) {
            throw new IllegalArgumentException("bidCount " + bidCount + " must be in [0, " + bidCache.size() + "]");
        }
        this.bidCount = bidCount;
    }

    public List<MutableRateLevel> getBids() {
        return bids;
    }

    public int getAskCount() {
        return askCount;
    }

    public void setAskCount(int askCount) {
        if (askCount < 0 || askCount > askCache.size()) {
            throw new IllegalArgumentException("askCount " + askCount + " must be in [0, " + askCache.size() + "]");
        }
        this.askCount = askCount;
    }

    public List<MutableRateLevel> getAsks() {
        return asks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MutableMarketDataSnapshot that = (MutableMarketDataSnapshot) o;
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
