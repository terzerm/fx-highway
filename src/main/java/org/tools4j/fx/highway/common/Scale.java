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
package org.tools4j.fx.highway.common;

public enum Scale {
    SCALE_0(0, 1L),
    SCALE_1(1, 10L),
    SCALE_2(2, 100L),
    SCALE_3(3, 1000L),
    SCALE_4(4, 10000L),
    SCALE_5(5, 100000L),
    SCALE_6(6, 1000000L),
    SCALE_7(7, 10000000L),
    SCALE_8(8, 100000000L),
    SCALE_9(9, 1000000000L),
    SCALE_10(10, 10000000000L);

    private final int scale;
    private final long multiplier;

    private Scale(final int scale, final long multiplier) {
        this.scale = scale;
        this.multiplier = multiplier;
    }

    public final int getScale() {
        return scale;
    }

    public final long getMultiplier() {
        return multiplier;
    }

    public double unscale(final double value) {
        return value / getMultiplier();
    }

    public double scale(final double value) {
        return value * getMultiplier();
    }

    public double scaleFrom(final double value, final Scale sourceScale) {
        if (this == sourceScale) return value;
        return rescale(value, sourceScale.getScale(), getScale());
    }

    private static volatile transient Scale[] UNIVERSE;

    private static Scale[] universe() {
        if (UNIVERSE == null) {
            UNIVERSE = values();
        }
        return UNIVERSE;
    }

    private static double rescale(final double value, final int sourceScale, final int targetScale) {
        final Scale[] universe = universe();
        return (targetScale >= sourceScale) ?
                universe[targetScale - sourceScale].scale(value) :
                universe[sourceScale - targetScale].unscale(value);
    }

    public static Scale valueOf(final int scale) {
        final Scale[] universe = universe();
        if (scale < 0 || scale >= universe.length) {
            throw new IllegalArgumentException("Invalid scale: " + scale);
        }
        return universe[scale];
    }
}
