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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.fileindex.SnapshotFileIndex.FileIndexScan;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestSnapshotFileIndex
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "day", Types.DateType.get()),
            Types.NestedField.optional(3, "name", Types.StringType.get()),
            Types.NestedField.optional(4, "amount", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(5, "score", Types.DoubleType.get()),
            Types.NestedField.optional(6, "bucket_key", Types.IntegerType.get()));
    private static final Map<String, IcebergColumnHandle> COLUMNS = getTopLevelColumns(SCHEMA, TESTING_TYPE_MANAGER).stream()
            .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    private static final List<String> INDEXED_COLUMNS = ImmutableList.of("id", "day", "name", "amount", "score");
    private static final Namespace NAMESPACE = Namespace.of("test_file_index");
    private static final int FIRST_DAY = 19_000;
    private static final int DAYS = 20;

    private final ExecutorService executor = newDirectExecutorService();
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
        executor.shutdownNow();
    }

    @Test
    void testUnpartitionedTable()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        appendRandomFiles(table, new Random(1), 300);

        assertMatchesPlanFiles(table, new Random(2), 400);
    }

    @Test
    void testIdentityPartitionedTable()
    {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("day").build());
        appendRandomFiles(table, new Random(3), 300);

        assertMatchesPlanFiles(table, new Random(4), 400);
    }

    @Test
    void testBucketPartitionedTable()
    {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("id", 8).build());
        appendRandomFiles(table, new Random(5), 300);

        assertMatchesPlanFiles(table, new Random(6), 400);
    }

    @Test
    void testPartitionSpecEvolution()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        Random random = new Random(7);
        appendRandomFiles(table, random, 100);
        table.updateSpec().addField("day").commit();
        appendRandomFiles(table, random, 100);
        table.updateSpec().addField(org.apache.iceberg.expressions.Expressions.bucket("id", 4)).commit();
        appendRandomFiles(table, random, 100);
        assertThat(table.specs()).hasSize(3);

        assertMatchesPlanFiles(table, new Random(8), 400);
    }

    @Test
    void testDeleteFiles()
    {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("day").build());
        appendRandomFiles(table, new Random(9), 100);
        for (int day = FIRST_DAY; day < FIRST_DAY + DAYS; day += 3) {
            DeleteFile deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                    .ofPositionDeletes()
                    .withPath("memory:///deletes/" + randomNameSuffix() + ".parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withPartition(new PartitionData(new Object[] {day}))
                    .build();
            table.newRowDelta().addDeletes(deleteFile).commit();
        }

        SnapshotFileIndex index = buildIndex(table, INDEXED_COLUMNS);
        Map<String, Set<String>> deletesByDataFile = deletesByDataFile(index.planFiles(alwaysTrue(), TupleDomain.all()).tasks());
        assertThat(deletesByDataFile).isEqualTo(deletesByDataFile(table.newScan().planFiles()));
        assertThat(deletesByDataFile.values()).anyMatch(deletes -> !deletes.isEmpty());

        assertMatchesPlanFiles(table, new Random(10), 200);
    }

    @Test
    void testIndexIsBoundToItsSnapshot()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        Random random = new Random(11);
        appendRandomFiles(table, random, 10);
        long firstSnapshot = table.currentSnapshot().snapshotId();
        appendRandomFiles(table, random, 5);

        SnapshotFileIndex firstIndex = SnapshotFileIndex.build(table, firstSnapshot, INDEXED_COLUMNS, TESTING_TYPE_MANAGER, executor, 1000).orElseThrow();
        assertThat(firstIndex.fileCount()).isEqualTo(10);
        assertThat(buildIndex(table, INDEXED_COLUMNS).fileCount()).isEqualTo(15);
    }

    @Test
    void testTooManyFiles()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        appendRandomFiles(table, new Random(12), 10);
        long snapshotId = table.currentSnapshot().snapshotId();

        assertThat(SnapshotFileIndex.build(table, snapshotId, INDEXED_COLUMNS, TESTING_TYPE_MANAGER, executor, 9)).isEmpty();
        assertThat(SnapshotFileIndex.build(table, snapshotId, INDEXED_COLUMNS, TESTING_TYPE_MANAGER, executor, 10)).isPresent();
    }

    @Test
    void testStatisticsColumns()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        appendRandomFiles(table, new Random(13), 50);

        // names are matched case insensitively; unknown columns are ignored
        SnapshotFileIndex index = buildIndex(table, ImmutableList.of("ID", "name", "no_such_column"));
        assertThat(index.hasStatisticsFor(ImmutableSet.of())).isTrue();
        assertThat(index.hasStatisticsFor(ImmutableSet.of(1, 3))).isTrue();
        assertThat(index.hasStatisticsFor(ImmutableSet.of(1, 2))).isFalse();

        // a filter on a column without statistics cannot prune, but must not lose files
        TupleDomain<IcebergColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMNS.get("id"), Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 300L)), false),
                COLUMNS.get("bucket_key"), Domain.singleValue(INTEGER, 3L)));
        Expression filter = toIcebergExpression(predicate);
        assertThat(paths(index.planFiles(filter, predicate).tasks()))
                .containsAll(paths(table.newScan().filter(filter).planFiles()));
    }

    @Test
    void testFilesKeepStatisticsOfListedColumns()
            throws IOException
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(
                        table,
                        "file",
                        null,
                        ImmutableMap.of(1, 10L, 3, "a", 5, 1.0),
                        ImmutableMap.of(1, 20L, 3, "b", 5, 2.0),
                        ImmutableMap.of(1, 0L, 3, 7L, 5, 0L)))
                .commit();

        // Split generation derives each split's statistics domain from these, so they must survive for the listed columns.
        // Other columns are dropped to save memory.
        SnapshotFileIndex index = buildIndex(table, ImmutableList.of("id", "name"));
        try (CloseableIterable<FileScanTask> tasks = index.planFiles(alwaysTrue(), TupleDomain.all()).tasks()) {
            DataFile file = getOnlyElement(tasks).file();
            assertThat(file.lowerBounds()).containsOnlyKeys(1, 3);
            assertThat(file.upperBounds()).containsOnlyKeys(1, 3);
            assertThat(file.nullValueCounts()).containsOnly(Map.entry(1, 0L), Map.entry(3, 7L));
            assertThat(Conversions.<Long>fromByteBuffer(Types.LongType.get(), file.lowerBounds().get(1))).isEqualTo(10L);
            assertThat(Conversions.<Long>fromByteBuffer(Types.LongType.get(), file.upperBounds().get(1))).isEqualTo(20L);
        }
    }

    @Test
    void testLargeInList()
    {
        Table table = createTable(PartitionSpec.unpartitioned());
        List<long[]> idBounds = new ArrayList<>();
        var append = table.newAppend();
        for (int file = 0; file < 200; file++) {
            long lower = file * 100L;
            long upper = lower + 9;
            idBounds.add(new long[] {lower, upper});
            append.appendFile(dataFile(table, "file_" + file, null, ImmutableMap.of(1, lower), ImmutableMap.of(1, upper), ImmutableMap.of()));
        }
        append.commit();

        // Iceberg stops evaluating IN lists against file statistics above 200 values, the index does not
        List<Long> values = new ArrayList<>();
        for (long value = 0; value < 300; value++) {
            values.add(value * 50 + 5);
        }
        TupleDomain<IcebergColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMNS.get("id"), Domain.multipleValues(BIGINT, values)));
        Expression filter = toIcebergExpression(predicate);

        List<String> expected = new ArrayList<>();
        for (int file = 0; file < idBounds.size(); file++) {
            long[] bounds = idBounds.get(file);
            if (values.stream().anyMatch(value -> value >= bounds[0] && value <= bounds[1])) {
                expected.add(table.location() + "/data/file_" + file + ".parquet");
            }
        }
        assertThat(expected).hasSizeBetween(1, 199);
        assertThat(paths(buildIndex(table, INDEXED_COLUMNS).planFiles(filter, predicate).tasks())).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(paths(table.newScan().filter(filter).planFiles())).containsAll(expected);
    }

    @Test
    void testScanCounters()
    {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("day").build());
        var append = table.newAppend();
        for (int day = 0; day < 4; day++) {
            for (int file = 0; file < 5; file++) {
                long lower = file * 100L;
                append.appendFile(dataFile(
                        table,
                        "day_" + day + "_file_" + file,
                        new PartitionData(new Object[] {FIRST_DAY + day}),
                        ImmutableMap.of(1, lower, 5, 1.0),
                        ImmutableMap.of(1, lower + 99, 5, (double) file),
                        ImmutableMap.of()));
            }
        }
        append.commit();

        // day keeps 2 of 4 partitions, id keeps 2 of the 5 files in each, score keeps 1 of those 2
        TupleDomain<IcebergColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMNS.get("day"), Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, FIRST_DAY + 2L)), false),
                COLUMNS.get("id"), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 250L, true, 399L, true)), false),
                COLUMNS.get("score"), Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 2.5)), false)));
        FileIndexScan scan = buildIndex(table, INDEXED_COLUMNS).planFiles(toIcebergExpression(predicate), predicate);

        assertThat(scan.indexedFiles()).isEqualTo(20);
        assertThat(scan.partitionCandidates()).isEqualTo(10);
        assertThat(scan.columnCandidates()).isEqualTo(4);
        assertThat(scan.matchedFiles()).hasValue(0);
        assertThat(paths(scan.tasks())).hasSize(2);
        assertThat(scan.matchedFiles()).hasValue(2);
    }

    private void assertMatchesPlanFiles(Table table, Random random, int queries)
    {
        SnapshotFileIndex index = buildIndex(table, INDEXED_COLUMNS);
        long prunedQueries = 0;
        for (int query = 0; query < queries; query++) {
            TupleDomain<IcebergColumnHandle> predicate = randomPredicate(random);
            Expression filter = toIcebergExpression(predicate);

            List<String> expected = paths(table.newScan().filter(filter).planFiles());
            assertThat(paths(index.planFiles(filter, predicate).tasks()))
                    .as("predicate %s", predicate)
                    .containsExactlyInAnyOrderElementsOf(expected);
            if (expected.size() < index.fileCount()) {
                prunedQueries++;
            }
        }
        // guards against a generator that only produces filters matching everything
        assertThat(prunedQueries).isGreaterThan(queries / 2);
    }

    private static TupleDomain<IcebergColumnHandle> randomPredicate(Random random)
    {
        ImmutableMap.Builder<IcebergColumnHandle, Domain> domains = ImmutableMap.builder();
        if (random.nextInt(3) != 0) {
            domains.put(COLUMNS.get("id"), randomDomain(random, BIGINT, () -> (long) random.nextInt(1100) - 50));
        }
        if (random.nextInt(3) == 0) {
            domains.put(COLUMNS.get("day"), randomDomain(random, DATE, () -> (long) FIRST_DAY - 2 + random.nextInt(DAYS + 4)));
        }
        if (random.nextInt(4) == 0) {
            domains.put(COLUMNS.get("name"), randomDomain(random, VARCHAR, () -> Slices.utf8Slice(randomName(random))));
        }
        if (random.nextInt(4) == 0) {
            Type amountType = COLUMNS.get("amount").getType();
            domains.put(COLUMNS.get("amount"), randomDomain(random, amountType, () -> (long) random.nextInt(110_000) - 5_000));
        }
        if (random.nextInt(6) == 0) {
            domains.put(COLUMNS.get("score"), randomDomain(random, DOUBLE, () -> random.nextInt(120) - 10.0));
        }
        return TupleDomain.withColumnDomains(domains.buildOrThrow());
    }

    private static Domain randomDomain(Random random, Type type, java.util.function.Supplier<Object> values)
    {
        boolean nullAllowed = random.nextInt(8) == 0;
        return switch (random.nextInt(7)) {
            case 0 -> Domain.create(ValueSet.of(type, values.get()), nullAllowed);
            case 1 -> {
                List<Object> list = new ArrayList<>();
                int count = 1 + random.nextInt(6);
                for (int i = 0; i < count; i++) {
                    list.add(values.get());
                }
                yield Domain.create(ValueSet.copyOf(type, list), nullAllowed);
            }
            case 2 -> Domain.create(ValueSet.ofRanges(Range.lessThan(type, values.get())), nullAllowed);
            case 3 -> Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, values.get())), nullAllowed);
            case 4 -> Domain.create(ValueSet.ofRanges(randomRange(random, type, values)), nullAllowed);
            case 5 -> Domain.create(ValueSet.ofRanges(randomRange(random, type, values), randomRange(random, type, values)), nullAllowed);
            default -> {
                if (random.nextBoolean()) {
                    yield Domain.onlyNull(type);
                }
                yield Domain.notNull(type);
            }
        };
    }

    private static Range randomRange(Random random, Type type, java.util.function.Supplier<Object> values)
    {
        Object first = values.get();
        Object second = values.get();
        Range lowToFirst = Range.lessThanOrEqual(type, first);
        // order the two values with the type's own ordering
        if (lowToFirst.contains(Range.equal(type, second))) {
            Object swap = first;
            first = second;
            second = swap;
        }
        if (Range.equal(type, first).equals(Range.equal(type, second))) {
            return Range.equal(type, first);
        }
        return Range.range(type, first, random.nextBoolean(), second, random.nextBoolean());
    }

    private void appendRandomFiles(Table table, Random random, int count)
    {
        var append = table.newAppend();
        for (int file = 0; file < count; file++) {
            int day = FIRST_DAY + random.nextInt(DAYS);
            long idLower = random.nextInt(1000);
            long idUpper = idLower + random.nextInt(60);

            Map<Integer, Object> lowerBounds = new HashMap<>();
            Map<Integer, Object> upperBounds = new HashMap<>();
            Map<Integer, Long> nullCounts = new HashMap<>();
            // a tenth of the files have no statistics for id
            if (random.nextInt(10) != 0) {
                lowerBounds.put(1, idLower);
                upperBounds.put(1, idUpper);
            }
            lowerBounds.put(2, day);
            upperBounds.put(2, day);
            if (random.nextInt(10) != 0) {
                String nameLower = randomName(random);
                lowerBounds.put(3, nameLower);
                upperBounds.put(3, nameLower + randomName(random));
                nullCounts.put(3, (long) random.nextInt(2));
            }
            else {
                // only NULLs
                nullCounts.put(3, 100L);
            }
            long amountLower = random.nextInt(100_000);
            lowerBounds.put(4, BigDecimal.valueOf(amountLower, 2));
            upperBounds.put(4, BigDecimal.valueOf(amountLower + random.nextInt(5_000), 2));
            nullCounts.put(4, (long) random.nextInt(2));
            double scoreLower = random.nextInt(100);
            lowerBounds.put(5, scoreLower);
            upperBounds.put(5, scoreLower + random.nextInt(10));

            append.appendFile(dataFile(table, "file_" + randomNameSuffix(), partition(table.spec(), day, idLower), lowerBounds, upperBounds, nullCounts));
        }
        append.commit();
    }

    private static PartitionData partition(PartitionSpec spec, int day, long id)
    {
        if (spec.isUnpartitioned()) {
            return null;
        }
        Object[] values = spec.fields().stream()
                .map(field -> {
                    if (field.transform().isIdentity()) {
                        return (Object) day;
                    }
                    // bucket transform
                    return (Object) (int) (id % 4);
                })
                .toArray();
        return new PartitionData(values);
    }

    private static DataFile dataFile(Table table, String name, PartitionData partition, Map<Integer, ?> lowerBounds, Map<Integer, ?> upperBounds, Map<Integer, Long> nullCounts)
    {
        Map<Integer, Long> valueCounts = SCHEMA.columns().stream()
                .collect(toImmutableMap(Types.NestedField::fieldId, _ -> 100L));
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1000)
                .withMetrics(new Metrics(100L, null, valueCounts, nullCounts, null, toByteBuffers(lowerBounds), toByteBuffers(upperBounds)));
        if (partition != null) {
            builder.withPartition(partition);
        }
        return builder.build();
    }

    private static Map<Integer, ByteBuffer> toByteBuffers(Map<Integer, ?> bounds)
    {
        ImmutableMap.Builder<Integer, ByteBuffer> buffers = ImmutableMap.builder();
        bounds.forEach((fieldId, value) -> buffers.put(fieldId, Conversions.toByteBuffer(SCHEMA.findType(fieldId), value)));
        return buffers.buildOrThrow();
    }

    private static String randomName(Random random)
    {
        StringBuilder name = new StringBuilder();
        int length = 1 + random.nextInt(3);
        for (int i = 0; i < length; i++) {
            name.append((char) ('a' + random.nextInt(6)));
        }
        return name.toString();
    }

    private Table createTable(PartitionSpec spec)
    {
        return catalog.buildTable(TableIdentifier.of(NAMESPACE, "table_" + randomNameSuffix()), SCHEMA)
                .withPartitionSpec(spec)
                .withProperty("format-version", "2")
                .create();
    }

    private SnapshotFileIndex buildIndex(Table table, List<String> columns)
    {
        Optional<SnapshotFileIndex> index = SnapshotFileIndex.build(table, table.currentSnapshot().snapshotId(), columns, TESTING_TYPE_MANAGER, executor, 100_000);
        assertThat(index).isPresent();
        return index.orElseThrow();
    }

    private static Expression alwaysTrue()
    {
        return org.apache.iceberg.expressions.Expressions.alwaysTrue();
    }

    private static List<String> paths(CloseableIterable<FileScanTask> tasks)
    {
        try (tasks) {
            return ImmutableList.copyOf(tasks).stream()
                    .map(task -> task.file().location())
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Set<String>> deletesByDataFile(CloseableIterable<FileScanTask> tasks)
    {
        try (tasks) {
            return ImmutableList.copyOf(tasks).stream()
                    .collect(toImmutableMap(
                            task -> task.file().location(),
                            task -> task.deletes().stream().map(DeleteFile::location).collect(toImmutableSet())));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
