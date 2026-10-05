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

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.TableMetadata;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Keeps loaded table metadata across transactions for a fixed duration. An entry is served until it
 * expires or is invalidated, even if the table has changed in the catalog since it was loaded.
 */
public class TableMetadataCache
{
    private static final long MAXIMUM_SIZE = 10_000;
    private static final TableMetadataCache DISABLED = new TableMetadataCache(Duration.ZERO, Ticker.systemTicker());

    private final Optional<Cache<SchemaTableName, TableMetadata>> cache;

    @Inject
    public TableMetadataCache(IcebergConfig config)
    {
        this(config.getTableMetadataCacheTtl(), Ticker.systemTicker());
    }

    public TableMetadataCache(Duration ttl, Ticker ticker)
    {
        requireNonNull(ttl, "ttl is null");
        requireNonNull(ticker, "ticker is null");
        if (ttl.toMillis() == 0) {
            this.cache = Optional.empty();
        }
        else {
            this.cache = Optional.of(EvictableCacheBuilder.newBuilder()
                    .expireAfterWrite(ttl.toMillis(), MILLISECONDS)
                    .maximumSize(MAXIMUM_SIZE)
                    .ticker(ticker)
                    .build());
        }
    }

    public static TableMetadataCache disabled()
    {
        return DISABLED;
    }

    public TableMetadata load(SchemaTableName table, Supplier<TableMetadata> loader)
    {
        if (cache.isEmpty()) {
            return loader.get();
        }
        try {
            return uncheckedCacheGet(cache.orElseThrow(), table, loader);
        }
        catch (UncheckedExecutionException e) {
            // Rethrow the loader's failure as is, so that callers see the same exceptions with and without the cache
            throwIfUnchecked(e.getCause());
            throw e;
        }
    }

    public void invalidate(SchemaTableName table)
    {
        cache.ifPresent(cache -> cache.invalidate(table));
    }
}
