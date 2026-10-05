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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TableMetadataCache;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.ZERO;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoNessieCatalog
        extends BaseTrinoCatalogTest
{
    private NessieContainer nessieContainer;

    @BeforeAll
    public void setupServer()
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
    }

    @AfterAll
    public void teardownServer()
    {
        if (nessieContainer != null) {
            nessieContainer.close();
        }
    }

    @Override
    protected void createNamespaceWithProperties(TrinoCatalog catalog, String namespace, Map<String, String> properties)
    {
        IcebergNessieCatalogConfig icebergNessieCatalogConfig = new IcebergNessieCatalogConfig()
                .setServerUri(URI.create(nessieContainer.getRestApiUri()));
        NessieApiV2 nessieApi = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV2.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, icebergNessieCatalogConfig.getDefaultReferenceName(), null, ImmutableMap.of());
        nessieClient.createNamespace(Namespace.of(namespace), properties);
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        Path tmpDirectory = null;
        try {
            tmpDirectory = createTempDirectory("test_nessie_catalog_warehouse_dir_");
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
        return createTrinoNessieCatalog(createNessieClient(), tmpDirectory, useUniqueTableLocations, TableMetadataCache.disabled());
    }

    private NessieIcebergClient createNessieClient()
    {
        IcebergNessieCatalogConfig icebergNessieCatalogConfig = new IcebergNessieCatalogConfig()
                .setServerUri(URI.create(nessieContainer.getRestApiUri()));
        NessieApiV2 nessieApi = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV2.class);
        return new NessieIcebergClient(nessieApi, icebergNessieCatalogConfig.getDefaultReferenceName(), null, ImmutableMap.of());
    }

    private static TrinoCatalog createTrinoNessieCatalog(NessieIcebergClient nessieClient, Path warehouseDirectory, boolean useUniqueTableLocations, TableMetadataCache tableMetadataCache)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
        return new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                TESTING_TYPE_MANAGER,
                fileSystemFactory,
                FILE_IO_FACTORY,
                new IcebergNessieTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY, nessieClient, ENCRYPTION_MANAGER_FACTORY, tableMetadataCache),
                nessieClient,
                warehouseDirectory.toAbsolutePath().toString(),
                useUniqueTableLocations,
                tableMetadataCache);
    }

    @Test
    public void testTableMetadataCache()
            throws Exception
    {
        Path warehouseDirectory = createTempDirectory("test_nessie_catalog_table_metadata_cache_");
        warehouseDirectory.toFile().deleteOnExit();

        TestingTicker ticker = new TestingTicker();
        TableMetadataCache tableMetadataCache = new TableMetadataCache(new Duration(5, MINUTES), ticker);
        // A catalog instance lives for one transaction, so each load below uses a new instance sharing the cache and the client
        NessieIcebergClient nessieClient = createNessieClient();
        Supplier<TrinoCatalog> cachingCatalog = () -> createTrinoNessieCatalog(nessieClient, warehouseDirectory, false, tableMetadataCache);
        // Stands for another engine changing the table
        NessieIcebergClient otherNessieClient = createNessieClient();
        Supplier<TrinoCatalog> otherCatalog = () -> createTrinoNessieCatalog(otherNessieClient, warehouseDirectory, false, TableMetadataCache.disabled());

        String namespace = "test_table_metadata_cache_" + randomNameSuffix();
        SchemaTableName table = new SchemaTableName(namespace, "cached_table");
        TrinoCatalog catalog = cachingCatalog.get();
        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            catalog.newCreateTableTransaction(
                            SESSION,
                            table,
                            new Schema(Types.NestedField.optional(1, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(arbitraryTableLocation(catalog, SESSION, table)),
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

    @Test
    public void testDefaultLocation()
            throws IOException
    {
        Path tmpDirectory = createTempDirectory("test_nessie_catalog_default_location_");
        tmpDirectory.toFile().deleteOnExit();
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
        IcebergNessieCatalogConfig icebergNessieCatalogConfig = new IcebergNessieCatalogConfig()
                .setDefaultWarehouseDir(tmpDirectory.toAbsolutePath().toString())
                .setServerUri(URI.create(nessieContainer.getRestApiUri()));
        NessieApiV2 nessieApi = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV2.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, icebergNessieCatalogConfig.getDefaultReferenceName(), null, ImmutableMap.of());
        TrinoCatalog catalogWithDefaultLocation = new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                TESTING_TYPE_MANAGER,
                fileSystemFactory,
                FILE_IO_FACTORY,
                new IcebergNessieTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY, nessieClient, ENCRYPTION_MANAGER_FACTORY, TableMetadataCache.disabled()),
                nessieClient,
                icebergNessieCatalogConfig.getDefaultWarehouseDir(),
                false,
                TableMetadataCache.disabled());

        String namespace = "test_default_location_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalogWithDefaultLocation.createNamespace(
                SESSION,
                namespace,
                ImmutableMap.of(),
                new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            File expectedSchemaDirectory = new File(tmpDirectory.toFile(), namespace);
            File expectedTableDirectory = new File(expectedSchemaDirectory, schemaTableName.getTableName());
            assertThat(catalogWithDefaultLocation.defaultTableLocation(SESSION, schemaTableName))
                    .isEqualTo(expectedTableDirectory.toPath().toAbsolutePath().toString());
        }
        finally {
            catalogWithDefaultLocation.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testViewNamespaceFilter()
    {
        assertThatThrownBy(super::testViewNamespaceFilter)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        catalog.createNamespace(SESSION, namespace, Map.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isTrue();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isFalse();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(schema)
                    .contains(namespace);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    new CatalogName("iceberg"),
                    PLANNER_CONTEXT.getTypeManager(),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (_, _) -> {
                        throw new UnsupportedOperationException();
                    },
                    TABLE_STATISTICS_READER,
                    new TableStatisticsWriter(new NodeVersion("test-version")),
                    UNSUPPORTED_DELETION_VECTOR_WRITER,
                    Optional.empty(),
                    false,
                    _ -> false,
                    newDirectExecutorService(),
                    directExecutor(),
                    newDirectExecutorService(),
                    newDirectExecutorService(),
                    0,
                    ZERO,
                    ConnectorExpressionEvaluator.NO_OP);
            assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isTrue();
            assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isFalse();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(schema)
                    .contains(namespace);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }
}
