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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.iceberg.util.FileOperationUtils.FileOperation;
import io.trino.plugin.iceberg.util.FileOperationUtils.FileType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.fileindex.SnapshotFileIndexManager.FILE_INDEX_COLUMNS;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.MANIFEST;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.SNAPSHOT;
import static io.trino.plugin.iceberg.util.FileOperationUtils.getOperations;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
final class TestIcebergFileIndexFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("test_schema")
                // table statistics are read from manifests, independently of split planning
                .setCatalogSessionProperty(ICEBERG_CATALOG, "statistics_enabled", "false")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();
        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.split-manager-threads", "0")
                .put("iceberg.metadata-cache.enabled", "false")
                .put("iceberg.file-index.enabled", "true")
                .put("iceberg.allowed-extra-properties", FILE_INDEX_COLUMNS)
                .buildOrThrow());
        queryRunner.execute("CREATE SCHEMA test_schema");
        return queryRunner;
    }

    @Test
    void testSplitsPlannedFromIndex()
    {
        String table = "test_file_index_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, name VARCHAR) WITH (extra_properties = MAP(ARRAY['" + FILE_INDEX_COLUMNS + "'], ARRAY['id']))");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (100, 'c'), (101, 'd')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (200, 'e'), (201, 'f')", 2);
        @Language("SQL") String query = "SELECT name FROM " + table + " WHERE id = 100";

        // the first query finds no index: it plans from manifests and starts the build
        assertThat(manifestReads(query)).isGreaterThan(0);
        assertQuery(query, "VALUES 'c'");
        // once the index is built, split planning reads neither the manifest list nor manifests
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 'c'");
        assertQuery("SELECT name FROM " + table + " WHERE id BETWEEN 2 AND 200", "VALUES 'b', 'c', 'd', 'e'");
        assertQuery("SELECT count(*) FROM " + table, "VALUES 6");
        assertQueryReturnsEmptyResult("SELECT name FROM " + table + " WHERE id = 50");

        // a filter on a column the index holds no statistics for is planned from manifests
        @Language("SQL") String nameQuery = "SELECT id FROM " + table + " WHERE name = 'c'";
        assertThat(manifestReads(nameQuery)).isGreaterThan(0);
        assertQuery(nameQuery, "VALUES 100");

        // a new snapshot is visible immediately, and gets its own index
        assertUpdate("INSERT INTO " + table + " VALUES (100, 'z')", 1);
        assertQuery(query, "VALUES 'c', 'z'");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 'c', 'z'");

        // row-level deletes are applied to splits planned from the index
        assertUpdate("DELETE FROM " + table + " WHERE name = 'z'", 1);
        assertQuery(query, "VALUES 'c'");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 'c'");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testVarcharColumn()
    {
        String table = "test_file_index_varchar_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, name VARCHAR) WITH (extra_properties = MAP(ARRAY['" + FILE_INDEX_COLUMNS + "'], ARRAY['name']))");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'apple'), (2, 'banana')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'cherry'), (4, 'date')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (5, 'éclair'), (6, '中文'), (7, '😀')", 3);
        // longer than the 16 characters Iceberg keeps of a string bound by default
        assertUpdate("INSERT INTO " + table + " VALUES (8, 'a_value_longer_than_the_bound_1'), (9, 'a_value_longer_than_the_bound_2')", 2);
        @Language("SQL") String query = "SELECT id FROM " + table + " WHERE name = 'cherry'";

        assertQuery(query, "VALUES 3");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 3");
        assertQuery("SELECT id FROM " + table + " WHERE name IN ('apple', 'date', '中文')", "VALUES 1, 4, 6");
        assertQuery("SELECT id FROM " + table + " WHERE name >= 'banana' AND name < 'd'", "VALUES 2, 3");
        assertQuery("SELECT id FROM " + table + " WHERE name > 'date'", "VALUES 5, 6, 7");
        assertQuery("SELECT id FROM " + table + " WHERE name LIKE 'a%'", "VALUES 1, 8, 9");
        assertQuery("SELECT id FROM " + table + " WHERE name = 'a_value_longer_than_the_bound_2'", "VALUES 9");
        assertQuery("SELECT id FROM " + table + " WHERE name = '😀'", "VALUES 7");
        assertQueryReturnsEmptyResult("SELECT id FROM " + table + " WHERE name = 'coconut'");
        assertQueryReturnsEmptyResult("SELECT id FROM " + table + " WHERE name IS NULL");
        assertQuery("SELECT count(*) FROM " + table + " WHERE name IS NOT NULL", "VALUES 9");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testAppendsAndPartitionDrops()
    {
        String table = "test_file_index_partitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, name VARCHAR, day DATE) " +
                "WITH (partitioning = ARRAY['day'], extra_properties = MAP(ARRAY['" + FILE_INDEX_COLUMNS + "'], ARRAY['name']))");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a', DATE '2024-01-01'), (2, 'b', DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'c', DATE '2024-01-02'), (4, 'd', DATE '2024-01-02')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (5, 'a', DATE '2024-01-03'), (6, 'e', DATE '2024-01-03')", 2);
        @Language("SQL") String query = "SELECT id FROM " + table + " WHERE name = 'a'";

        assertQuery(query, "VALUES 1, 5");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 1, 5");

        // each new snapshot gets its index from the previous one; results must be right before and after it is ready
        for (int day = 4; day <= 8; day++) {
            assertUpdate("INSERT INTO " + table + " VALUES (" + (day * 10) + ", 'a', DATE '2024-01-0" + day + "'), (" + (day * 10 + 1) + ", 'f', DATE '2024-01-0" + day + "')", 2);
        }
        assertQuery(query, "VALUES 1, 5, 40, 50, 60, 70, 80");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 1, 5, 40, 50, 60, 70, 80");

        // dropping whole partitions removes files without writing delete files
        assertUpdate("DELETE FROM " + table + " WHERE day < DATE '2024-01-03'", 4);
        assertQuery(query, "VALUES 5, 40, 50, 60, 70, 80");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 5, 40, 50, 60, 70, 80");
        assertQuery("SELECT id FROM " + table + " WHERE name = 'a' AND day = DATE '2024-01-05'", "VALUES 50");
        assertQueryReturnsEmptyResult("SELECT id FROM " + table + " WHERE name = 'c'");
        assertQuery("SELECT count(*) FROM " + table, "VALUES 12");

        assertUpdate("INSERT INTO " + table + " VALUES (90, 'a', DATE '2024-01-09')", 1);
        assertQuery(query, "VALUES 5, 40, 50, 60, 70, 80, 90");
        assertEventually(() -> assertThat(manifestReads(query)).isZero());
        assertQuery(query, "VALUES 5, 40, 50, 60, 70, 80, 90");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testJoinWithDynamicFilter()
    {
        String indexedTable = "test_file_index_dynamic_filter_" + randomNameSuffix();
        String plainTable = "test_no_file_index_dynamic_filter_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + indexedTable + " (id BIGINT, name VARCHAR, code VARCHAR) WITH (extra_properties = MAP(ARRAY['" + FILE_INDEX_COLUMNS + "'], ARRAY['name']))");
        assertUpdate("CREATE TABLE " + plainTable + " (id BIGINT, name VARCHAR, code VARCHAR)");
        for (String table : ImmutableList.of(indexedTable, plainTable)) {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'a', 'u'), (2, 'b', 'v')", 2);
            assertUpdate("INSERT INTO " + table + " VALUES (3, 'c', 'w'), (4, 'd', 'x')", 2);
            assertUpdate("INSERT INTO " + table + " VALUES (5, 'e', 'y'), (6, 'f', 'z')", 2);
        }
        // split generation waits for the dynamic filter, so the filter is known when the files are looked up
        Session dynamicFiltering = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "dynamic_filtering_wait_timeout", "1m")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .build();
        Session noDynamicFiltering = Session.builder(dynamicFiltering)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .build();

        // The build side value is only known when the query runs. With a constant, the optimizer would turn the join
        // condition into a static filter on the table, and leave nothing to the dynamic filter.
        // The dynamic filter is on the indexed column.
        @Language("SQL") String onName = "SELECT t.id FROM %s t JOIN (SELECT IF(rand() >= 0, 'c') AS name) d ON t.name = d.name";
        assertQuery(dynamicFiltering, onName.formatted(indexedTable), "VALUES 3");
        assertEventually(() -> assertThat(manifestReads(dynamicFiltering, onName.formatted(indexedTable))).isZero());
        assertQuery(dynamicFiltering, onName.formatted(indexedTable), "VALUES 3");
        // the files it rules out are not opened, the same as without the index
        int dataFileReads = dataFileReads(dynamicFiltering, onName.formatted(indexedTable));
        assertThat(dataFileReads).isEqualTo(dataFileReads(dynamicFiltering, onName.formatted(plainTable)));
        assertThat(dataFileReads).isLessThan(dataFileReads(noDynamicFiltering, onName.formatted(indexedTable)));
        assertQuery(noDynamicFiltering, onName.formatted(indexedTable), "VALUES 3");

        // the dynamic filter is on a column the index holds no statistics for: the scan is planned from manifests,
        // and the dynamic filter prunes files as it does without the index
        @Language("SQL") String onCode = "SELECT t.id FROM %s t JOIN (SELECT IF(rand() >= 0, 'x') AS code) d ON t.code = d.code";
        assertQuery(dynamicFiltering, onCode.formatted(indexedTable), "VALUES 4");
        assertThat(manifestReads(dynamicFiltering, onCode.formatted(indexedTable))).isGreaterThan(0);
        assertThat(dataFileReads(dynamicFiltering, onCode.formatted(indexedTable)))
                .isEqualTo(dataFileReads(dynamicFiltering, onCode.formatted(plainTable)))
                .isLessThan(dataFileReads(noDynamicFiltering, onCode.formatted(indexedTable)));

        assertUpdate("DROP TABLE " + indexedTable);
        assertUpdate("DROP TABLE " + plainTable);
    }

    @Test
    void testTableWithoutIndexedColumnsIsPlannedFromManifests()
    {
        String table = "test_no_file_index_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        @Language("SQL") String query = "SELECT name FROM " + table + " WHERE id = 1";

        for (int i = 0; i < 3; i++) {
            assertThat(manifestReads(query)).isGreaterThan(0);
        }
        assertQuery(query, "VALUES 'a'");

        assertUpdate("DROP TABLE " + table);
    }

    private int manifestReads(@Language("SQL") String query)
    {
        return manifestReads(getSession(), query);
    }

    private int manifestReads(Session session, @Language("SQL") String query)
    {
        Multiset<FileType> readFileTypes = readFileTypes(session, query);
        return readFileTypes.count(MANIFEST) + readFileTypes.count(SNAPSHOT);
    }

    private int dataFileReads(Session session, @Language("SQL") String query)
    {
        return readFileTypes(session, query).count(DATA);
    }

    private synchronized Multiset<FileType> readFileTypes(Session session, @Language("SQL") String query)
    {
        getDistributedQueryRunner().executeWithPlan(session, query);
        return getOperations(getDistributedQueryRunner().getSpans()).stream()
                .filter(operation -> operation.operationType().startsWith("InputFile."))
                .map(FileOperation::fileType)
                .collect(toImmutableMultiset());
    }
}
