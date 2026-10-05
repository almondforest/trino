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

import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Immutable index over closed intervals that finds the intervals overlapping a query range.
 * <p>
 * Intervals are kept sorted by lower bound. A binary tree laid out over that order stores the greatest upper
 * bound of every subtree, so a query skips the intervals that start after the range ends with a binary search,
 * and skips whole subtrees that end before the range starts.
 */
final class IntervalIndex
{
    private final Comparator<Object> comparator;
    // interval ids and lower bounds, sorted by lower bound
    private final int[] ids;
    private final Object[] lowers;
    // heap-ordered tree over the sorted intervals: the leaf of sorted position i is at leafOffset + i
    private final Object[] maxUppers;
    private final int leafOffset;

    IntervalIndex(Comparator<Object> comparator, int[] ids, List<Object> lowers, List<Object> uppers)
    {
        this.comparator = requireNonNull(comparator, "comparator is null");
        checkArgument(ids.length == lowers.size() && ids.length == uppers.size(), "ids, lowers and uppers must have the same size");

        int size = ids.length;
        Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (left, right) -> comparator.compare(lowers.get(left), lowers.get(right)));

        this.ids = new int[size];
        this.lowers = new Object[size];
        this.leafOffset = Math.max(1, Integer.highestOneBit(Math.max(1, size - 1)) << 1);
        this.maxUppers = new Object[2 * leafOffset];
        for (int i = 0; i < size; i++) {
            int source = order[i];
            this.ids[i] = ids[source];
            this.lowers[i] = requireNonNull(lowers.get(source), "lower is null");
            this.maxUppers[leafOffset + i] = requireNonNull(uppers.get(source), "upper is null");
        }
        for (int node = leafOffset - 1; node >= 1; node--) {
            maxUppers[node] = max(maxUppers[2 * node], maxUppers[2 * node + 1]);
        }
    }

    int size()
    {
        return ids.length;
    }

    /**
     * Sets the bit of every interval that shares at least one value with the given range.
     * A null bound means the range is unbounded on that side.
     */
    void collectOverlapping(@Nullable Object low, boolean lowInclusive, @Nullable Object high, boolean highInclusive, BitSet result)
    {
        int end = countStartingWithin(high, highInclusive);
        if (end == 0) {
            return;
        }
        if (low == null) {
            for (int i = 0; i < end; i++) {
                result.set(ids[i]);
            }
            return;
        }
        collect(1, 0, leafOffset, end, low, lowInclusive, result);
    }

    /**
     * Number of intervals, in sorted order, whose lower bound does not lie past the high end of the range.
     */
    private int countStartingWithin(@Nullable Object high, boolean highInclusive)
    {
        if (high == null) {
            return ids.length;
        }
        int from = 0;
        int to = ids.length;
        while (from < to) {
            int middle = (from + to) >>> 1;
            int comparison = comparator.compare(lowers[middle], high);
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

    private void collect(int node, int start, int stop, int end, Object low, boolean lowInclusive, BitSet result)
    {
        if (start >= end) {
            return;
        }
        Object maxUpper = maxUppers[node];
        if (maxUpper == null) {
            return;
        }
        int comparison = comparator.compare(maxUpper, low);
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

    @Nullable
    private Object max(@Nullable Object left, @Nullable Object right)
    {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        if (comparator.compare(left, right) >= 0) {
            return left;
        }
        return right;
    }
}
