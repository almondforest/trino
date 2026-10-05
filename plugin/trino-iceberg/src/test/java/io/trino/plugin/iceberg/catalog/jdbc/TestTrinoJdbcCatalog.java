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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.catalog.TableMetadataCache;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.Driver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion.V1;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestTrinoJdbcCatalog
{
    private static final String CATALOG_NAME = "iceberg_jdbc";
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private TestingIcebergJdbcServer server;

    @BeforeAll
    void setUp()
    {
        server = closer.register(new TestingIcebergJdbcServer());
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        closer.close();
    }

    private static TrinoJdbcCatalog createTrinoJdbcCatalog(
            boolean useUniqueTableLocations,
            Path warehouseLocation,
            String jdbcUrl,
            JdbcCatalog jdbcCatalog,
            TableMetadataCache tableMetadataCache)
    {
        IcebergJdbcClient jdbcClient = new IcebergJdbcClient(
                new IcebergJdbcConnectionFactory(new Driver(), jdbcUrl, Optional.of(USER), Optional.of(PASSWORD)),
                CATALOG_NAME,
                V1);
        return new TrinoJdbcCatalog(
                new CatalogName(CATALOG_NAME),
                TESTING_TYPE_MANAGER,
                new IcebergJdbcTableOperationsProvider(HDFS_FILE_SYSTEM_FACTORY, FILE_IO_FACTORY, jdbcClient, tableMetadataCache),
                jdbcCatalog,
                jdbcClient,
                HDFS_FILE_SYSTEM_FACTORY,
                FILE_IO_FACTORY,
                useUniqueTableLocations,
                warehouseLocation.toAbsolutePath().toString(),
                tableMetadataCache);
    }

    private static JdbcCatalog createJdbcCatalog(String jdbcUrl, Path warehouseLocation)
    {
        return (JdbcCatalog) buildIcebergCatalog(
                CATALOG_NAME,
                ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, jdbcUrl)
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.toAbsolutePath().toString())
                        .buildOrThrow(),
                null);
    }

    @Test
    void testTableMetadataCache()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();
        JdbcCatalog jdbcCatalog = createJdbcCatalog(server.getJdbcUrl(), warehouseLocation);
        closer.register(jdbcCatalog);

        TestingTicker ticker = new TestingTicker();
        TableMetadataCache tableMetadataCache = new TableMetadataCache(new Duration(5, MINUTES), ticker);
        // A catalog instance lives for one transaction, so each load below uses a new instance sharing the cache
        Supplier<TrinoCatalog> cachingCatalog = () -> createTrinoJdbcCatalog(false, warehouseLocation, server.getJdbcUrl(), jdbcCatalog, tableMetadataCache);
        // Stands for another engine changing the table
        Supplier<TrinoCatalog> otherCatalog = () -> createTrinoJdbcCatalog(false, warehouseLocation, server.getJdbcUrl(), jdbcCatalog, TableMetadataCache.disabled());

        String namespace = "test_table_metadata_cache_" + randomNameSuffix();
        SchemaTableName table = new SchemaTableName(namespace, "cached_table");
        TrinoCatalog catalog = cachingCatalog.get();
        catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            catalog.newCreateTableTransaction(
                            SESSION,
                            table,
                            new Schema(Types.NestedField.optional(1, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(catalog.defaultTableLocation(SESSION, table)),
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties()).doesNotContainKey("marker");

            // a change made elsewhere stays invisible while the entry is alive
            otherCatalog.get().loadTable(SESSION, table).updateProperties().set("marker", "external").commit();
            ticker.increment(4, MINUTES);
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties()).doesNotContainKey("marker");

            // and shows up once the entry expires
            ticker.increment(2, MINUTES);
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties()).containsEntry("marker", "external");

            // a change made through the caching catalog is visible immediately
            cachingCatalog.get().loadTable(SESSION, table).updateProperties().set("marker", "own").commit();
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties()).containsEntry("marker", "own");

            // a commit that starts from stale cached metadata still succeeds and keeps the other engine's change
            otherCatalog.get().loadTable(SESSION, table).updateProperties().set("external_marker", "external").commit();
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties()).doesNotContainKey("external_marker");
            cachingCatalog.get().loadTable(SESSION, table).updateProperties().set("marker", "own again").commit();
            assertThat(cachingCatalog.get().loadTable(SESSION, table).properties())
                    .containsEntry("marker", "own again")
                    .containsEntry("external_marker", "external");

            // dropping the table removes the entry
            cachingCatalog.get().dropTable(SESSION, table);
            assertThatThrownBy(() -> cachingCatalog.get().loadTable(SESSION, table))
                    .isInstanceOf(TableNotFoundException.class);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }
}
