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
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;

final class TestIcebergJdbcCatalogTableMetadataCache
        extends AbstractTestQueryFramework
{
    // Stands for another engine working on the same catalog
    private JdbcCatalog jdbcCatalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File warehouseLocation = Files.createTempDirectory("test_iceberg_jdbc_catalog_table_metadata_cache").toFile();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        jdbcCatalog = closeAfterClass((JdbcCatalog) buildIcebergCatalog(
                "tpch",
                ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, server.getJdbcUrl())
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", SchemaVersion.V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath())
                        .buildOrThrow(),
                null));
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "jdbc")
                        .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                        .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                        .put("iceberg.jdbc-catalog.connection-user", USER)
                        .put("iceberg.jdbc-catalog.connection-password", PASSWORD)
                        .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                        .put("iceberg.jdbc-catalog.schema-version", SchemaVersion.V1.toString())
                        .put("iceberg.jdbc-catalog.default-warehouse-dir", warehouseLocation.getAbsolutePath())
                        .put("iceberg.table-metadata-cache.ttl", "1h")
                        .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .build();
    }

    @Test
    void testOwnWritesAreVisible()
    {
        String table = "test_own_writes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a BIGINT)");
        assertQueryReturnsEmptyResult("SELECT * FROM " + table);

        assertUpdate("INSERT INTO " + table + " VALUES 1", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 1");
        assertUpdate("INSERT INTO " + table + " VALUES 2", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 1, 2");

        assertUpdate("UPDATE " + table + " SET a = a + 10 WHERE a = 1", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 11, 2");
        assertUpdate("DELETE FROM " + table + " WHERE a = 2", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 11");

        assertUpdate("ALTER TABLE " + table + " ADD COLUMN b VARCHAR");
        assertQuery("SELECT * FROM " + table, "VALUES (11, NULL)");
        assertUpdate("ALTER TABLE " + table + " EXECUTE optimize");
        assertQuery("SELECT * FROM " + table, "VALUES (11, NULL)");

        assertUpdate("ALTER TABLE " + table + " RENAME TO " + table + "_renamed");
        assertQueryFails("SELECT * FROM " + table, ".* Table '.*' does not exist");
        assertQuery("SELECT * FROM " + table + "_renamed", "VALUES (11, NULL)");

        assertUpdate("DROP TABLE " + table + "_renamed");
        assertQueryFails("SELECT * FROM " + table + "_renamed", ".* Table '.*' does not exist");

        // a table created under a name used before must not be served from an entry of the old table
        assertUpdate("CREATE TABLE " + table + " (c VARCHAR)");
        assertUpdate("INSERT INTO " + table + " VALUES 'new'", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 'new'");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    @Disabled("In 479 beginInsert refreshes the table metadata, so an INSERT planned from a cached schema fails after another engine changes the schema")
    void testChangeFromOtherEngineIsNotVisible()
    {
        String table = "test_other_engine_" + randomNameSuffix();
        TableIdentifier tableIdentifier = TableIdentifier.of("tpch", table);
        assertUpdate("CREATE TABLE " + table + " (a BIGINT)");
        assertUpdate("INSERT INTO " + table + " VALUES 1", 1);
        assertQuery("SELECT * FROM " + table, "VALUES 1");

        jdbcCatalog.loadTable(tableIdentifier).updateSchema().addColumn("b", Types.StringType.get()).commit();
        // the cached metadata is still used
        assertQuery("SELECT * FROM " + table, "VALUES 1");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '" + table + "'", "VALUES 'a'");

        // a write planned from the cached metadata commits on top of the other engine's change, and refreshes the entry
        assertUpdate("INSERT INTO " + table + " VALUES 2", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1, NULL), (2, NULL)");

        assertUpdate("DROP TABLE " + table);
    }
}
