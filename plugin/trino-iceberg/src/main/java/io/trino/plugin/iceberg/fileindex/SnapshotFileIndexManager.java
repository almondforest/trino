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

import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.cache.NonEvictableCache;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.plugin.iceberg.ForIcebergSplitSource;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

/**
 * Builds and keeps {@link SnapshotFileIndex} instances for tables that declare the columns to index in the
 * {@value #FILE_INDEX_COLUMNS} table property.
 * <p>
 * An index is built in the background the first time a snapshot is asked for. Until it is ready, callers get
 * no index and plan the scan from manifests as usual. Only the most recently indexed snapshot of a table is kept.
 */
public class SnapshotFileIndexManager
{
    public static final String FILE_INDEX_COLUMNS = "trino.file-index.columns";

    private static final Logger log = Logger.get(SnapshotFileIndexManager.class);
    private static final Splitter COLUMN_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final boolean enabled;
    private final long maxFiles;
    private final TypeManager typeManager;
    private final Executor buildExecutor;
    private final ExecutorService planningExecutor;
    private final Cache<IndexKey, SnapshotFileIndex> indexes;
    // Snapshots that are too large or failed to load, remembered for a while to avoid rebuilding on every query
    private final NonEvictableCache<IndexKey, Boolean> rejected = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(Duration.ofMinutes(10)));
    private final Set<IndexKey> building = ConcurrentHashMap.newKeySet();

    @Inject
    public SnapshotFileIndexManager(
            IcebergConfig config,
            TypeManager typeManager,
            @ForIcebergSplitSource ListeningExecutorService buildExecutor,
            @ForIcebergSplitManager ExecutorService planningExecutor)
    {
        this(config.isFileIndexEnabled(), config.getFileIndexMaxFiles(), typeManager, buildExecutor, planningExecutor);
    }

    public SnapshotFileIndexManager(boolean enabled, long maxFiles, TypeManager typeManager, Executor buildExecutor, ExecutorService planningExecutor)
    {
        this.enabled = enabled;
        this.maxFiles = maxFiles;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.buildExecutor = requireNonNull(buildExecutor, "buildExecutor is null");
        this.planningExecutor = requireNonNull(planningExecutor, "planningExecutor is null");
        this.indexes = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxFiles)
                // The cache splits the weight limit evenly between its segments, and drops an entry heavier than
                // a segment's share as soon as it is added. One segment lets a single index use the whole limit.
                .concurrencyLevel(1)
                .weigher((IndexKey _, SnapshotFileIndex index) -> Math.max(1, index.fileCount()))
                .build();
    }

    /**
     * Returns the index of the snapshot if the table asks for one and it has been built.
     * Otherwise starts building it, unless that is already in progress, and returns empty.
     */
    public Optional<SnapshotFileIndex> find(Table table, long snapshotId)
    {
        if (!enabled) {
            return Optional.empty();
        }
        String columns = table.properties().get(FILE_INDEX_COLUMNS);
        if (columns == null) {
            return Optional.empty();
        }
        IndexKey key = new IndexKey(table.location(), snapshotId, COLUMN_SPLITTER.splitToList(columns));

        SnapshotFileIndex index = indexes.getIfPresent(key);
        if (index != null) {
            return Optional.of(index);
        }
        // The background build reads manifests through a detached copy of the table, which cannot decrypt them
        if (!(table.encryption() instanceof PlaintextEncryptionManager) || !(table instanceof HasTableOperations tableWithOperations)) {
            return Optional.empty();
        }
        if (rejected.getIfPresent(key) != null || !building.add(key)) {
            return Optional.empty();
        }

        // The table belongs to the calling transaction and is not thread safe, so the build gets its own immutable view of it
        Table detachedTable = new BaseTable(new StaticTableOperations(tableWithOperations.operations().current(), table.io()), table.name());
        try {
            buildExecutor.execute(() -> build(key, detachedTable));
        }
        catch (RejectedExecutionException e) {
            building.remove(key);
            return Optional.empty();
        }
        // Present already if the executor ran the build on this thread
        return Optional.ofNullable(indexes.getIfPresent(key));
    }

    private void build(IndexKey key, Table table)
    {
        try {
            Optional<SnapshotFileIndex> index = SnapshotFileIndex.build(table, key.snapshotId(), key.columns(), typeManager, planningExecutor, maxFiles);
            if (index.isEmpty()) {
                log.info("Not indexing snapshot %s of table %s: it has more than %s data files", key.snapshotId(), table.name(), maxFiles);
                rejected.put(key, true);
                return;
            }
            uncheckedCacheGet(indexes, key, index::orElseThrow);
            // Queries move on to the new snapshot, so keeping the indexes of older ones would only hold memory.
            // A query that is still planning from an older index keeps its own reference to it.
            invalidateAllIf(indexes, other -> other.tableLocation().equals(key.tableLocation()) && !other.equals(key));
            log.debug("Indexed %s data files of snapshot %s of table %s", index.orElseThrow().fileCount(), key.snapshotId(), table.name());
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to index snapshot %s of table %s", key.snapshotId(), table.name());
            rejected.put(key, true);
        }
        finally {
            building.remove(key);
        }
    }

    private record IndexKey(String tableLocation, long snapshotId, List<String> columns)
    {
        IndexKey
        {
            requireNonNull(tableLocation, "tableLocation is null");
            columns = List.copyOf(columns);
        }
    }
}
