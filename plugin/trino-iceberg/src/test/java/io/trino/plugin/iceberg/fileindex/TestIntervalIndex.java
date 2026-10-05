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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.types.Comparators;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIntervalIndex
{
    // Includes the boundaries between one, two, three and four byte encodings, and characters on both sides of the surrogate range
    private static final int[] CODE_POINTS = {'a', 'b', 'z', 0x7F, 0x80, 0xE9, 0x7FF, 0x800, 0x4E2D, 0xD7FF, 0xE000, 0xFFFD, 0xFFFF, 0x10000, 0x1F600, 0x10FFFF};

    @Test
    void testEmpty()
    {
        IntervalIndex<Integer> index = IntervalIndex.forValues(Comparator.<Integer>naturalOrder(), new int[0], ImmutableList.of(), ImmutableList.of());
        assertThat(index.size()).isZero();
        assertThat(overlapping(index, null, false, null, false)).isEmpty();
        assertThat(overlapping(index, 1, true, 10, true)).isEmpty();

        IntervalIndex<Slice> utf8Index = IntervalIndex.forUtf8(new int[0], ImmutableList.of(), ImmutableList.of());
        assertThat(utf8Index.size()).isZero();
        assertThat(overlapping(utf8Index, null, false, null, false)).isEmpty();
        assertThat(overlapping(utf8Index, slice("a"), true, slice("b"), true)).isEmpty();
    }

    @Test
    void testSingleInterval()
    {
        IntervalIndex<Integer> index = intervals(new int[] {7}, ImmutableList.of(10), ImmutableList.of(20));

        assertThat(overlapping(index, null, false, null, false)).containsExactly(7);
        assertThat(overlapping(index, 15, true, 15, true)).containsExactly(7);
        assertThat(overlapping(index, 0, true, 9, true)).isEmpty();
        assertThat(overlapping(index, 21, true, 30, true)).isEmpty();
    }

    @Test
    void testBoundInclusiveness()
    {
        IntervalIndex<Integer> index = intervals(new int[] {0}, ImmutableList.of(10), ImmutableList.of(20));

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
        IntervalIndex<Integer> index = intervals(
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
            int[] ids = shuffledIds(size);
            List<Integer> lowers = new ArrayList<>();
            List<Integer> uppers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                int lower = random.nextInt(100);
                lowers.add(lower);
                uppers.add(lower + random.nextInt(30));
            }
            IntervalIndex<Integer> index = intervals(ids, lowers, uppers);
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

                assertThat(overlapping(index, low, lowInclusive, high, highInclusive))
                        .as("size %s, range %s (inclusive %s) to %s (inclusive %s)", size, low, lowInclusive, high, highInclusive)
                        .containsExactlyInAnyOrderElementsOf(linearScan(Comparator.<Integer>naturalOrder(), ids, lowers, uppers, low, lowInclusive, high, highInclusive));
            }
        }
    }

    @Test
    void testUtf8BoundInclusiveness()
    {
        IntervalIndex<Slice> index = IntervalIndex.forUtf8(new int[] {0}, ImmutableList.of(buffer("b")), ImmutableList.of(buffer("d")));

        assertThat(overlapping(index, slice("a"), true, slice("b"), true)).containsExactly(0);
        assertThat(overlapping(index, slice("a"), true, slice("b"), false)).isEmpty();
        assertThat(overlapping(index, slice("d"), true, slice("e"), true)).containsExactly(0);
        assertThat(overlapping(index, slice("d"), false, slice("e"), true)).isEmpty();
        assertThat(overlapping(index, slice("c"), true, slice("c"), true)).containsExactly(0);
        // a string sorts after each of its prefixes
        assertThat(overlapping(index, slice("da"), true, null, false)).isEmpty();
        assertThat(overlapping(index, null, false, slice("az"), true)).isEmpty();
        assertThat(overlapping(index, null, false, slice("ba"), true)).containsExactly(0);
        assertThat(overlapping(index, slice(""), true, slice(""), true)).isEmpty();
    }

    @Test
    void testUtf8EmptyStringBounds()
    {
        IntervalIndex<Slice> index = IntervalIndex.forUtf8(
                new int[] {0, 1},
                ImmutableList.of(buffer(""), buffer("")),
                ImmutableList.of(buffer(""), buffer("b")));

        assertThat(overlapping(index, slice(""), true, slice(""), true)).containsExactly(0, 1);
        assertThat(overlapping(index, slice(""), false, null, false)).containsExactly(1);
        assertThat(overlapping(index, slice("a"), true, slice("a"), true)).containsExactly(1);
    }

    @Test
    void testUtf8BoundsAreReadWithoutMovingBuffers()
    {
        // bounds as they can come from file metadata: views into a larger array, and read-only
        byte[] backing = "xxbdxx".getBytes(UTF_8);
        ByteBuffer lower = ByteBuffer.wrap(backing, 2, 1);
        ByteBuffer upper = ByteBuffer.wrap(backing, 3, 1).slice().asReadOnlyBuffer();

        IntervalIndex<Slice> index = IntervalIndex.forUtf8(new int[] {5}, ImmutableList.of(lower), ImmutableList.of(upper));
        assertThat(lower.position()).isEqualTo(2);
        assertThat(lower.remaining()).isEqualTo(1);
        assertThat(upper.position()).isZero();
        assertThat(upper.remaining()).isEqualTo(1);

        assertThat(overlapping(index, slice("c"), true, slice("c"), true)).containsExactly(5);
        assertThat(overlapping(index, slice("e"), true, null, false)).isEmpty();
        assertThat(overlapping(index, null, false, slice("a"), true)).isEmpty();

        // the query value can be a view into a larger slice too
        Slice value = Slices.utf8Slice("xxcxx").slice(2, 1);
        assertThat(overlapping(index, value, true, value, true)).containsExactly(5);
    }

    /**
     * File pruning by Iceberg compares decoded strings with this comparator. The index compares encoded bytes,
     * and must select exactly the same intervals.
     */
    @Test
    void testUtf8AgainstLinearScanWithIcebergStringOrder()
    {
        Comparator<CharSequence> icebergOrder = Comparators.charSequences();
        Random random = new Random(11);
        for (int size : new int[] {1, 2, 3, 7, 64, 65, 400}) {
            int[] ids = shuffledIds(size);
            List<String> lowers = new ArrayList<>();
            List<String> uppers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                String first = randomString(random);
                String second = randomString(random);
                if (icebergOrder.compare(first, second) <= 0) {
                    lowers.add(first);
                    uppers.add(second);
                }
                else {
                    lowers.add(second);
                    uppers.add(first);
                }
            }
            IntervalIndex<Slice> index = IntervalIndex.forUtf8(
                    ids,
                    lowers.stream().map(TestIntervalIndex::buffer).toList(),
                    uppers.stream().map(TestIntervalIndex::buffer).toList());
            assertThat(index.size()).isEqualTo(size);

            for (int query = 0; query < 500; query++) {
                String low = null;
                if (random.nextInt(5) != 0) {
                    low = randomString(random);
                }
                String high = null;
                if (random.nextInt(5) != 0) {
                    high = randomString(random);
                }
                boolean lowInclusive = random.nextBoolean();
                boolean highInclusive = random.nextBoolean();

                assertThat(overlapping(index, slice(low), lowInclusive, slice(high), highInclusive))
                        .as("size %s, range %s (inclusive %s) to %s (inclusive %s)", size, low, lowInclusive, high, highInclusive)
                        .containsExactlyInAnyOrderElementsOf(linearScan(icebergOrder, ids, lowers, uppers, low, lowInclusive, high, highInclusive));
            }
        }
    }

    private static <V, T extends V> List<Integer> linearScan(Comparator<V> comparator, int[] ids, List<T> lowers, List<T> uppers, T low, boolean lowInclusive, T high, boolean highInclusive)
    {
        List<Integer> matching = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            if (low != null) {
                int comparison = comparator.compare(uppers.get(i), low);
                if (comparison < 0 || (comparison == 0 && !lowInclusive)) {
                    continue;
                }
            }
            if (high != null) {
                int comparison = comparator.compare(lowers.get(i), high);
                if (comparison > 0 || (comparison == 0 && !highInclusive)) {
                    continue;
                }
            }
            matching.add(ids[i]);
        }
        return matching;
    }

    private static IntervalIndex<Integer> intervals(int[] ids, List<Integer> lowers, List<Integer> uppers)
    {
        return IntervalIndex.forValues(Comparator.<Integer>naturalOrder(), ids, lowers, uppers);
    }

    private static <T> List<Integer> overlapping(IntervalIndex<T> index, T low, boolean lowInclusive, T high, boolean highInclusive)
    {
        BitSet result = new BitSet();
        index.collectOverlapping(low, lowInclusive, high, highInclusive, result);
        return result.stream().boxed().toList();
    }

    // ids are deliberately not the positions, and not in lower bound order
    private static int[] shuffledIds(int size)
    {
        int[] ids = new int[size];
        for (int i = 0; i < size; i++) {
            ids[i] = size - 1 - i;
        }
        return ids;
    }

    private static String randomString(Random random)
    {
        StringBuilder string = new StringBuilder();
        int length = random.nextInt(4);
        for (int i = 0; i < length; i++) {
            string.appendCodePoint(CODE_POINTS[random.nextInt(CODE_POINTS.length)]);
        }
        return string.toString();
    }

    private static ByteBuffer buffer(String value)
    {
        return ByteBuffer.wrap(value.getBytes(UTF_8));
    }

    private static Slice slice(String value)
    {
        if (value == null) {
            return null;
        }
        return Slices.utf8Slice(value);
    }
}
