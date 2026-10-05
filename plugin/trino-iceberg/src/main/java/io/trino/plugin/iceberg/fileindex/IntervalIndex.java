/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.fileindex;

import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Immutable index over closed intervals that finds the intervals overlapping a query range.
 * <p>
 * Intervals are kept sorted by lower bound. A binary tree laid out over that order records, for every subtree,
 * which interval has the greatest upper bound, so a query skips the intervals that start after the range ends
 * with a binary search, and skips whole subtrees that end before the range starts.
 *
 * @param <T> type of the bounds of a query range
 */
final class IntervalIndex<T>
{
    private static final int NO_INTERVAL = -1;

    // bounds, and the ids of their intervals, in lower bound order
    private final Bounds<T> bounds;
    private final int[] ids;
    // heap-ordered tree over the sorted intervals: the leaf of sorted position i is at leafOffset + i,
    // and every node holds the sorted position of the interval with the greatest upper bound below it
    private final int[] maxUpperPositions;
    private final int leafOffset;

    private IntervalIndex(Bounds<T> bounds, int[] ids)
    {
        this.bounds = requireNonNull(bounds, "bounds is null");
        this.ids = requireNonNull(ids, "ids is null");

        int size = ids.length;
        this.leafOffset = Math.max(1, Integer.highestOneBit(Math.max(1, size - 1)) << 1);
        this.maxUpperPositions = new int[2 * leafOffset];
        Arrays.fill(maxUpperPositions, NO_INTERVAL);
        for (int position = 0; position < size; position++) {
            maxUpperPositions[leafOffset + position] = position;
        }
        for (int node = leafOffset - 1; node >= 1; node--) {
            maxUpperPositions[node] = positionOfGreaterUpper(maxUpperPositions[2 * node], maxUpperPositions[2 * node + 1]);
        }
    }

    /**
     * Index over bounds held as objects and ordered by the given comparator.
     */
    static <T> IntervalIndex<T> forValues(Comparator<T> comparator, int[] ids, List<T> lowers, List<T> uppers)
    {
        requireNonNull(comparator, "comparator is null");
        checkArgument(ids.length == lowers.size() && ids.length == uppers.size(), "ids, lowers and uppers must have the same size");

        int[] order = sortedOrder(ids.length, (left, right) -> comparator.compare(lowers.get(left), lowers.get(right)));
        Object[] sortedLowers = new Object[ids.length];
        Object[] sortedUppers = new Object[ids.length];
        for (int position = 0; position < ids.length; position++) {
            sortedLowers[position] = requireNonNull(lowers.get(order[position]), "lower is null");
            sortedUppers[position] = requireNonNull(uppers.get(order[position]), "upper is null");
        }
        return new IntervalIndex<>(new ValueBounds<>(comparator, sortedLowers, sortedUppers), reorder(ids, order));
    }

    /**
     * Index over UTF-8 encoded string bounds, ordered by code point. All bounds are packed into two byte arrays,
     * and are compared with the bytes of the query bound without decoding either side.
     */
    static IntervalIndex<Slice> forUtf8(int[] ids, List<ByteBuffer> lowers, List<ByteBuffer> uppers)
    {
        checkArgument(ids.length == lowers.size() && ids.length == uppers.size(), "ids, lowers and uppers must have the same size");

        int[] order = sortedOrder(ids.length, (left, right) -> compareUnsigned(lowers.get(left), lowers.get(right)));
        return new IntervalIndex<>(new Utf8Bounds(PackedBytes.pack(lowers, order), PackedBytes.pack(uppers, order)), reorder(ids, order));
    }

    int size()
    {
        return ids.length;
    }

    /**
     * Sets the bit of every interval that shares at least one value with the given range.
     * A null bound means the range is unbounded on that side.
     */
    void collectOverlapping(@Nullable T low, boolean lowInclusive, @Nullable T high, boolean highInclusive, BitSet result)
    {
        int end = countStartingWithin(high, highInclusive);
        if (end == 0) {
            return;
        }
        if (low == null) {
            for (int position = 0; position < end; position++) {
                result.set(ids[position]);
            }
            return;
        }
        collect(1, 0, leafOffset, end, low, lowInclusive, result);
    }

    /**
     * Number of intervals, in sorted order, whose lower bound does not lie past the high end of the range.
     */
    private int countStartingWithin(@Nullable T high, boolean highInclusive)
    {
        if (high == null) {
            return ids.length;
        }
        int from = 0;
        int to = ids.length;
        while (from < to) {
            int middle = (from + to) >>> 1;
            int comparison = bounds.compareLowerTo(middle, high);
            boolean startsWithin = comparison < 0 || (highInclusive && comparison == 0);
            if (startsWithin) {
                from = middle + 1;
            }
            else {
                to = middle;
            }
        }
        return from;
    }

    private void collect(int node, int start, int stop, int end, T low, boolean lowInclusive, BitSet result)
    {
        if (start >= end) {
            return;
        }
        int maxUpperPosition = maxUpperPositions[node];
        if (maxUpperPosition == NO_INTERVAL) {
            return;
        }
        int comparison = bounds.compareUpperTo(maxUpperPosition, low);
        boolean reachesRange = comparison > 0 || (lowInclusive && comparison == 0);
        if (!reachesRange) {
            return;
        }
        if (stop - start == 1) {
            result.set(ids[start]);
            return;
        }
        int middle = (start + stop) >>> 1;
        collect(2 * node, start, middle, end, low, lowInclusive, result);
        collect(2 * node + 1, middle, stop, end, low, lowInclusive, result);
    }

    private int positionOfGreaterUpper(int left, int right)
    {
        if (left == NO_INTERVAL) {
            return right;
        }
        if (right == NO_INTERVAL) {
            return left;
        }
        if (bounds.compareUppers(left, right) >= 0) {
            return left;
        }
        return right;
    }

    private static int[] sortedOrder(int size, Comparator<Integer> comparator)
    {
        Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        Arrays.sort(order, comparator);
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = order[i];
        }
        return result;
    }

    private static int[] reorder(int[] values, int[] order)
    {
        int[] result = new int[values.length];
        for (int position = 0; position < values.length; position++) {
            result[position] = values[order[position]];
        }
        return result;
    }

    private static int compareUnsigned(ByteBuffer left, ByteBuffer right)
    {
        // ByteBuffer.compareTo compares bytes as signed values, which is not the order of UTF-8 encoded strings
        int mismatch = left.mismatch(right);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch >= left.remaining() || mismatch >= right.remaining()) {
            return Integer.compare(left.remaining(), right.remaining());
        }
        return Byte.compareUnsigned(left.get(left.position() + mismatch), right.get(right.position() + mismatch));
    }

    /**
     * Bounds of the indexed intervals, addressed by position in lower bound order.
     */
    private interface Bounds<T>
    {
        int compareUppers(int leftPosition, int rightPosition);

        int compareLowerTo(int position, T value);

        int compareUpperTo(int position, T value);
    }

    private record ValueBounds<T>(Comparator<T> comparator, Object[] lowers, Object[] uppers)
            implements Bounds<T>
    {
        @Override
        public int compareUppers(int leftPosition, int rightPosition)
        {
            return comparator.compare(upper(leftPosition), upper(rightPosition));
        }

        @Override
        public int compareLowerTo(int position, T value)
        {
            return comparator.compare(lower(position), value);
        }

        @Override
        public int compareUpperTo(int position, T value)
        {
            return comparator.compare(upper(position), value);
        }

        @SuppressWarnings("unchecked")
        private T lower(int position)
        {
            return (T) lowers[position];
        }

        @SuppressWarnings("unchecked")
        private T upper(int position)
        {
            return (T) uppers[position];
        }
    }

    private record Utf8Bounds(PackedBytes lowers, PackedBytes uppers)
            implements Bounds<Slice>
    {
        @Override
        public int compareUppers(int leftPosition, int rightPosition)
        {
            return uppers.compare(leftPosition, rightPosition);
        }

        @Override
        public int compareLowerTo(int position, Slice value)
        {
            return lowers.compareTo(position, value);
        }

        @Override
        public int compareUpperTo(int position, Slice value)
        {
            return uppers.compareTo(position, value);
        }
    }

    /**
     * Byte strings stored back to back: entry i occupies bytes[offsets[i]] up to bytes[offsets[i + 1]].
     */
    private record PackedBytes(byte[] bytes, int[] offsets)
    {
        static PackedBytes pack(List<ByteBuffer> values, int[] order)
        {
            int[] offsets = new int[order.length + 1];
            long totalLength = 0;
            for (int position = 0; position < order.length; position++) {
                totalLength += requireNonNull(values.get(order[position]), "value is null").remaining();
                // The largest array a JVM is certain to allocate is slightly smaller than Integer.MAX_VALUE
                checkArgument(totalLength <= Integer.MAX_VALUE - 8, "Bounds are too large to index: more than %s bytes", Integer.MAX_VALUE - 8);
                offsets[position + 1] = (int) totalLength;
            }
            byte[] bytes = new byte[(int) totalLength];
            for (int position = 0; position < order.length; position++) {
                ByteBuffer value = values.get(order[position]);
                // absolute get: the buffer belongs to the file's metadata and its position must not move
                value.get(value.position(), bytes, offsets[position], value.remaining());
            }
            return new PackedBytes(bytes, offsets);
        }

        int compare(int leftPosition, int rightPosition)
        {
            return Arrays.compareUnsigned(
                    bytes,
                    offsets[leftPosition],
                    offsets[leftPosition + 1],
                    bytes,
                    offsets[rightPosition],
                    offsets[rightPosition + 1]);
        }

        int compareTo(int position, Slice value)
        {
            return Arrays.compareUnsigned(
                    bytes,
                    offsets[position],
                    offsets[position + 1],
                    value.byteArray(),
                    value.byteArrayOffset(),
                    value.byteArrayOffset() + value.length());
        }
    }
}
