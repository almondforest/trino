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
package io.trino.plugin.iceberg.catalog;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTableMetadataCache
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final Duration TTL = new Duration(5, MINUTES);

    @Test
    void testServesCachedMetadataUntilExpiry()
    {
        TestingTicker ticker = new TestingTicker();
        TableMetadataCache cache = new TableMetadataCache(TTL, ticker);
        TableMetadata first = tableMetadata("first");
        TableMetadata second = tableMetadata("second");
        AtomicInteger loads = new AtomicInteger();

        assertThat(cache.load(TABLE, () -> counted(loads, first))).isSameAs(first);
        // the table changed, but the entry is still within its lifetime
        ticker.increment(4, MINUTES);
        assertThat(cache.load(TABLE, () -> counted(loads, second))).isSameAs(first);
        assertThat(loads).hasValue(1);

        ticker.increment(2, MINUTES);
        assertThat(cache.load(TABLE, () -> counted(loads, second))).isSameAs(second);
        assertThat(loads).hasValue(2);
    }

    @Test
    void testExpiryIsNotExtendedByReads()
    {
        TestingTicker ticker = new TestingTicker();
        TableMetadataCache cache = new TableMetadataCache(TTL, ticker);
        TableMetadata first = tableMetadata("first");
        TableMetadata second = tableMetadata("second");

        assertThat(cache.load(TABLE, () -> first)).isSameAs(first);
        ticker.increment(3, MINUTES);
        assertThat(cache.load(TABLE, () -> second)).isSameAs(first);
        ticker.increment(3, MINUTES);
        assertThat(cache.load(TABLE, () -> second)).isSameAs(second);
    }

    @Test
    void testInvalidate()
    {
        TableMetadataCache cache = new TableMetadataCache(TTL, new TestingTicker());
        SchemaTableName otherTable = new SchemaTableName("schema", "other");
        TableMetadata first = tableMetadata("first");
        TableMetadata second = tableMetadata("second");

        assertThat(cache.load(TABLE, () -> first)).isSameAs(first);
        assertThat(cache.load(otherTable, () -> first)).isSameAs(first);

        cache.invalidate(TABLE);
        assertThat(cache.load(TABLE, () -> second)).isSameAs(second);
        assertThat(cache.load(otherTable, () -> second)).isSameAs(first);
    }

    @Test
    void testDisabled()
    {
        TableMetadataCache cache = TableMetadataCache.disabled();
        TableMetadata first = tableMetadata("first");
        TableMetadata second = tableMetadata("second");

        assertThat(cache.load(TABLE, () -> first)).isSameAs(first);
        assertThat(cache.load(TABLE, () -> second)).isSameAs(second);
        cache.invalidate(TABLE);
    }

    @Test
    void testLoaderFailureIsRethrownAndNotCached()
    {
        TableMetadataCache cache = new TableMetadataCache(TTL, new TestingTicker());
        TableMetadata metadata = tableMetadata("first");

        assertThatThrownBy(() -> cache.load(TABLE, () -> {
            throw new TableNotFoundException(TABLE);
        })).isInstanceOf(TableNotFoundException.class);
        assertThat(cache.load(TABLE, () -> metadata)).isSameAs(metadata);
    }

    private static TableMetadata counted(AtomicInteger loads, TableMetadata metadata)
    {
        loads.incrementAndGet();
        return metadata;
    }

    private static TableMetadata tableMetadata(String marker)
    {
        return TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.optional(1, "col1", Types.LongType.get())),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "memory:///" + marker,
                ImmutableMap.of("marker", marker));
    }
}
