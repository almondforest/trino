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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.fileindex.SnapshotFileIndex.BuildStatistics;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.iceberg.fileindex.SnapshotFileIndexManager.FILE_INDEX_COLUMNS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestSnapshotFileIndexManager
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    private static final Namespace NAMESPACE = Namespace.of("test_file_index_manager");

    private final ExecutorService planningExecutor = newDirectExecutorService();
    private InMemoryCatalog catalog;

    @BeforeAll
    void setUp()
    {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(NAMESPACE);
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        catalog.close();
        planningExecutor.shutdownNow();
    }

    @Test
    void testIndexIsBuiltOncePerSnapshot()
    {
        List<Runnable> submitted = new ArrayList<>();
        Executor buildExecutor = task -> {
            submitted.add(task);
            task.run();
        };
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, buildExecutor, planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 3);
        long firstSnapshot = table.currentSnapshot().snapshotId();

        Optional<SnapshotFileIndex> index = manager.find(table, firstSnapshot);
        assertThat(index).isPresent();
        assertThat(index.orElseThrow().fileCount()).isEqualTo(3);
        assertThat(manager.find(table, firstSnapshot)).containsSame(index.orElseThrow());
        assertThat(submitted).hasSize(1);

        appendFiles(table, 2);
        long secondSnapshot = table.currentSnapshot().snapshotId();
        Optional<SnapshotFileIndex> newIndex = manager.find(table, secondSnapshot);
        assertThat(newIndex).isPresent();
        assertThat(newIndex.orElseThrow().fileCount()).isEqualTo(5);
        assertThat(manager.find(table, secondSnapshot)).containsSame(newIndex.orElseThrow());
        assertThat(submitted).hasSize(2);
    }

    @Test
    void testNewSnapshotIsBuiltFromPreviousIndex()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table table = createTable(Optional.of("id"));
        fastAppendFiles(table, 3);
        fastAppendFiles(table, 3);
        SnapshotFileIndex index = manager.find(table, table.currentSnapshot().snapshotId()).orElseThrow();
        assertThat(index.buildStatistics()).isEqualTo(new BuildStatistics(true, 2, 0));

        fastAppendFiles(table, 2);
        SnapshotFileIndex newIndex = manager.find(table, table.currentSnapshot().snapshotId()).orElseThrow();
        assertThat(newIndex.fileCount()).isEqualTo(8);
        assertThat(newIndex.buildStatistics()).isEqualTo(new BuildStatistics(true, 1, 2));

        // an index of other columns holds files with other statistics, and is not built upon
        table.updateProperties().set(FILE_INDEX_COLUMNS, "name").commit();
        SnapshotFileIndex nameIndex = manager.find(table, table.currentSnapshot().snapshotId()).orElseThrow();
        assertThat(nameIndex.buildStatistics()).isEqualTo(new BuildStatistics(true, 3, 0));
    }

    @Test
    void testOnlyLatestIndexedSnapshotOfTableIsKept()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 3);
        long firstSnapshot = table.currentSnapshot().snapshotId();
        Table otherTable = createTable(Optional.of("id"));
        appendFiles(otherTable, 1);
        long otherTableSnapshot = otherTable.currentSnapshot().snapshotId();

        SnapshotFileIndex firstIndex = manager.find(table, firstSnapshot).orElseThrow();
        SnapshotFileIndex otherTableIndex = manager.find(otherTable, otherTableSnapshot).orElseThrow();

        appendFiles(table, 2);
        assertThat(manager.find(table, table.currentSnapshot().snapshotId())).isPresent();

        // the index of the superseded snapshot was dropped, so asking for it again builds a new one
        assertThat(manager.find(table, firstSnapshot))
                .isPresent()
                .get().isNotSameAs(firstIndex);
        // a holder of the dropped index can keep using it
        assertThat(firstIndex.fileCount()).isEqualTo(3);
        assertThat(manager.find(otherTable, otherTableSnapshot)).containsSame(otherTableIndex);
    }

    @Test
    void testIndexIsBuiltInBackground()
    {
        List<Runnable> submitted = new ArrayList<>();
        Executor buildExecutor = submitted::add;
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, buildExecutor, planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 3);
        long snapshotId = table.currentSnapshot().snapshotId();

        // not ready: the caller plans from manifests, and only one build is started
        assertThat(manager.find(table, snapshotId)).isEmpty();
        assertThat(manager.find(table, snapshotId)).isEmpty();
        assertThat(submitted).hasSize(1);

        submitted.getFirst().run();
        assertThat(manager.find(table, snapshotId)).isPresent();
        assertThat(submitted).hasSize(1);
    }

    @Test
    void testNotIndexedWithoutTableProperty()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table table = createTable(Optional.empty());
        appendFiles(table, 3);

        assertThat(manager.find(table, table.currentSnapshot().snapshotId())).isEmpty();
    }

    @Test
    void testDisabled()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(false, 1000, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 3);

        assertThat(manager.find(table, table.currentSnapshot().snapshotId())).isEmpty();
    }

    @Test
    void testChangedColumnsRebuildIndex()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 3);
        long snapshotId = table.currentSnapshot().snapshotId();

        SnapshotFileIndex index = manager.find(table, snapshotId).orElseThrow();
        assertThat(index.hasStatisticsFor(java.util.Set.of(2))).isFalse();

        table.updateProperties().set(FILE_INDEX_COLUMNS, "id, name").commit();
        SnapshotFileIndex newIndex = manager.find(table, snapshotId).orElseThrow();
        assertThat(newIndex).isNotSameAs(index);
        assertThat(newIndex.hasStatisticsFor(java.util.Set.of(1, 2))).isTrue();
    }

    @Test
    void testSnapshotWithTooManyFilesIsNotIndexedOrRetried()
    {
        List<Runnable> submitted = new ArrayList<>();
        Executor buildExecutor = task -> {
            submitted.add(task);
            task.run();
        };
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 4, TESTING_TYPE_MANAGER, buildExecutor, planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 5);
        long snapshotId = table.currentSnapshot().snapshotId();

        assertThat(manager.find(table, snapshotId)).isEmpty();
        assertThat(manager.find(table, snapshotId)).isEmpty();
        assertThat(submitted).hasSize(1);
    }

    @Test
    void testSnapshotCloseToMaxFilesStaysIndexed()
    {
        List<Runnable> submitted = new ArrayList<>();
        Executor buildExecutor = task -> {
            submitted.add(task);
            task.run();
        };
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 1000, TESTING_TYPE_MANAGER, buildExecutor, planningExecutor);
        Table table = createTable(Optional.of("id"));
        appendFiles(table, 1000);
        long snapshotId = table.currentSnapshot().snapshotId();

        SnapshotFileIndex index = manager.find(table, snapshotId).orElseThrow();
        assertThat(index.fileCount()).isEqualTo(1000);
        assertThat(manager.find(table, snapshotId)).containsSame(index);
        assertThat(submitted).hasSize(1);
    }

    @Test
    void testIndexesAreEvictedToStayWithinMaxFiles()
    {
        SnapshotFileIndexManager manager = new SnapshotFileIndexManager(true, 5, TESTING_TYPE_MANAGER, directExecutor(), planningExecutor);
        Table firstTable = createTable(Optional.of("id"));
        appendFiles(firstTable, 4);
        Table secondTable = createTable(Optional.of("id"));
        appendFiles(secondTable, 4);

        SnapshotFileIndex firstIndex = manager.find(firstTable, firstTable.currentSnapshot().snapshotId()).orElseThrow();
        assertThat(manager.find(secondTable, secondTable.currentSnapshot().snapshotId())).isPresent();
        // both do not fit, so the first one was evicted and is built again
        assertThat(manager.find(firstTable, firstTable.currentSnapshot().snapshotId()))
                .isPresent()
                .get().isNotSameAs(firstIndex);
    }

    private Table createTable(Optional<String> indexedColumns)
    {
        var builder = catalog.buildTable(TableIdentifier.of(NAMESPACE, "table_" + randomNameSuffix()), SCHEMA);
        indexedColumns.ifPresent(columns -> builder.withProperty(FILE_INDEX_COLUMNS, columns));
        return builder.create();
    }

    private static void appendFiles(Table table, int count)
    {
        appendFiles(table.newAppend(), table, count);
    }

    // always writes one new manifest and leaves the others alone
    private static void fastAppendFiles(Table table, int count)
    {
        appendFiles(table.newFastAppend(), table, count);
    }

    private static void appendFiles(AppendFiles append, Table table, int count)
    {
        for (int file = 0; file < count; file++) {
            append.appendFile(DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/" + randomNameSuffix() + ".parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(1000)
                    .withRecordCount(100)
                    .build());
        }
        append.commit();
    }
}
