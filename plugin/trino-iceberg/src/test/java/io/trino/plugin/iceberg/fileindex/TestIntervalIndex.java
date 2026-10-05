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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

final class TestIntervalIndex
{
    private static final Comparator<Object> COMPARATOR = (left, right) -> Integer.compare((int) left, (int) right);

    @Test
    void testEmpty()
    {
        IntervalIndex index = new IntervalIndex(COMPARATOR, new int[0], ImmutableList.of(), ImmutableList.of());

        assertThat(index.size()).isZero();
        assertThat(overlapping(index, null, false, null, false)).isEmpty();
        assertThat(overlapping(index, 1, true, 10, true)).isEmpty();
    }

    @Test
    void testSingleInterval()
    {
        IntervalIndex index = new IntervalIndex(COMPARATOR, new int[] {7}, ImmutableList.of(10), ImmutableList.of(20));

        assertThat(overlapping(index, null, false, null, false)).containsExactly(7);
        assertThat(overlapping(index, 15, true, 15, true)).containsExactly(7);
        assertThat(overlapping(index, 0, true, 9, true)).isEmpty();
        assertThat(overlapping(index, 21, true, 30, true)).isEmpty();
    }

    @Test
    void testBoundInclusiveness()
    {
        IntervalIndex index = new IntervalIndex(COMPARATOR, new int[] {0}, ImmutableList.of(10), ImmutableList.of(20));

        // range ends exactly where the interval starts
        assertThat(overlapping(index, 0, true, 10, true)).containsExactly(0);
        assertThat(overlapping(index, 0, true, 10, false)).isEmpty();
        assertThat(overlapping(index, null, false, 10, true)).containsExactly(0);
        assertThat(overlapping(index, null, false, 10, false)).isEmpty();

        // range starts exactly where the interval ends
        assertThat(overlapping(index, 20, true, 30, true)).containsExactly(0);
        assertThat(overlapping(index, 20, false, 30, true)).isEmpty();
        assertThat(overlapping(index, 20, true, null, false)).containsExactly(0);
        assertThat(overlapping(index, 20, false, null, false)).isEmpty();
    }

    @Test
    void testNestedAndDuplicateIntervals()
    {
        IntervalIndex index = new IntervalIndex(
                COMPARATOR,
                new int[] {0, 1, 2, 3, 4},
                ImmutableList.of(0, 10, 10, 40, 45),
                ImmutableList.of(100, 20, 20, 50, 46));

        // the wide interval is found even though intervals starting after it end before the range
        assertThat(overlapping(index, 60, true, 70, true)).containsExactly(0);
        assertThat(overlapping(index, 15, true, 15, true)).containsExactly(0, 1, 2);
        assertThat(overlapping(index, 21, true, 39, true)).containsExactly(0);
        assertThat(overlapping(index, 46, false, 49, true)).containsExactly(0, 3);
        assertThat(overlapping(index, 101, true, null, false)).isEmpty();
    }

    @Test
    void testAgainstLinearScan()
    {
        Random random = new Random(7);
        for (int size : new int[] {1, 2, 3, 4, 5, 8, 9, 31, 32, 33, 500}) {
            int[] ids = new int[size];
            List<Object> lowers = new ArrayList<>();
            List<Object> uppers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                // ids are deliberately not the positions, and not in lower bound order
                ids[i] = size - 1 - i;
                int lower = random.nextInt(100);
                lowers.add(lower);
                uppers.add(lower + random.nextInt(30));
            }
            IntervalIndex index = new IntervalIndex(COMPARATOR, ids, lowers, uppers);
            assertThat(index.size()).isEqualTo(size);

            for (int query = 0; query < 500; query++) {
                Integer low = null;
                if (random.nextInt(5) != 0) {
                    low = random.nextInt(140) - 5;
                }
                Integer high = null;
                if (random.nextInt(5) != 0) {
                    high = random.nextInt(140) - 5;
                }
                boolean lowInclusive = random.nextBoolean();
                boolean highInclusive = random.nextBoolean();

                List<Integer> expected = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    if (overlaps((int) lowers.get(i), (int) uppers.get(i), low, lowInclusive, high, highInclusive)) {
                        expected.add(ids[i]);
                    }
                }
                assertThat(overlapping(index, low, lowInclusive, high, highInclusive))
                        .as("size %s, range %s (inclusive %s) to %s (inclusive %s)", size, low, lowInclusive, high, highInclusive)
                        .containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    private static boolean overlaps(int lower, int upper, Integer low, boolean lowInclusive, Integer high, boolean highInclusive)
    {
        if (low != null && (upper < low || (upper == low && !lowInclusive))) {
            return false;
        }
        if (high != null && (lower > high || (lower == high && !highInclusive))) {
            return false;
        }
        return true;
    }

    private static List<Integer> overlapping(IntervalIndex index, Integer low, boolean lowInclusive, Integer high, boolean highInclusive)
    {
        BitSet result = new BitSet();
        index.collectOverlapping(low, lowInclusive, high, highInclusive, result);
        return result.stream().boxed().toList();
    }
}
