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
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.function.Function.identity;

/**
 * Times finding the data files of a query in the metadata of a large table: with a table scan, which is what split
 * generation does without an index, and with {@link SnapshotFileIndex}.
 * <p>
 * Only table metadata is written, on local disk. No data files exist, and none are needed: neither way of planning
 * opens them. The table is partitioned by day, and within a day every file covers a narrow range of {@code customer},
 * as it would after sorted writes.
 * <p>
 * Usage: {@code BenchmarkSnapshotFileIndex <empty directory> [days] [files per day]}
 */
public final class BenchmarkSnapshotFileIndex
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "customer", Types.StringType.get()),
            Types.NestedField.optional(3, "day", Types.DateType.get()),
            Types.NestedField.optional(4, "event_time", Types.TimestampType.withZone()),
            Types.NestedField.optional(5, "amount", Types.DoubleType.get()),
            Types.NestedField.optional(6, "status", Types.StringType.get()),
            Types.NestedField.optional(7, "quantity", Types.LongType.get()),
            Types.NestedField.optional(8, "price", Types.DoubleType.get()),
            Types.NestedField.optional(9, "region", Types.StringType.get()),
            Types.NestedField.optional(10, "note", Types.StringType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("day").build();
    private static final Map<String, IcebergColumnHandle> COLUMNS = getTopLevelColumns(SCHEMA, TESTING_TYPE_MANAGER).stream()
            .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    private static final List<String> INDEXED_COLUMNS = ImmutableList.of("customer");
    private static final int FIRST_DAY = 19_000;
    private static final int CUSTOMERS_PER_FILE = 100;
    private static final int COMMITS = 100;
    private static final int BUILD_RUNS = 3;
    private static final long MAX_FILES = 1_000_000_000;

    private final Table table;
    private final ExecutorService executor;
    private final int days;
    private final int filesPerDay;

    private BenchmarkSnapshotFileIndex(Table table, ExecutorService executor, int days, int filesPerDay)
    {
        this.table = table;
        this.executor = executor;
        this.days = days;
        this.filesPerDay = filesPerDay;
    }

    public static void main(String[] args)
            throws Exception
    {
        Path directory = Path.of(args[0]);
        int days = 1000;
        if (args.length > 1) {
            days = Integer.parseInt(args[1]);
        }
        int filesPerDay = 1000;
        if (args.length > 2) {
            filesPerDay = Integer.parseInt(args[2]);
        }
        // the default number of threads Trino plans splits with
        int threads = Math.min(Runtime.getRuntime().availableProcessors() * 2, 32);
        ExecutorService executor = newFixedThreadPool(threads, daemonThreadsNamed("benchmark-planning-%s"));

        Table table = new HadoopTables().create(SCHEMA, SPEC, ImmutableMap.of("format-version", "2"), directory.resolve("table").toString());
        BenchmarkSnapshotFileIndex benchmark = new BenchmarkSnapshotFileIndex(table, executor, days, filesPerDay);
        report("planning threads", "%s", threads);
        benchmark.writeTable();
        benchmark.run();
        executor.shutdownNow();
    }

    private void writeTable()
            throws Exception
    {
        long start = System.nanoTime();
        int daysPerCommit = Math.max(1, days / COMMITS);
        for (int day = 0; day < days; day += daysPerCommit) {
            // a merging append, as Trino does by default, so that manifests come out the way they would in a real table
            AppendFiles append = table.newAppend();
            for (int commitDay = day; commitDay < Math.min(day + daysPerCommit, days); commitDay++) {
                for (int file = 0; file < filesPerDay; file++) {
                    append.appendFile(dataFile(FIRST_DAY + commitDay, file));
                }
            }
            append.commit();
        }
        report("write table metadata", "%s", seconds(System.nanoTime() - start));

        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        long manifestBytes = manifests.stream().mapToLong(ManifestFile::length).sum();
        report("data files", "%,d", (long) days * filesPerDay);
        report("data manifests", "%,d files, %,d MB in total, largest %,d MB", manifests.size(), manifestBytes >> 20, manifests.stream().mapToLong(ManifestFile::length).max().orElse(0) >> 20);
    }

    private void run()
            throws Exception
    {
        String customer = customer((filesPerDay / 2) * CUSTOMERS_PER_FILE + 7);
        Domain customerDomain = Domain.singleValue(VARCHAR, Slices.utf8Slice(customer));
        TupleDomain<IcebergColumnHandle> customerOnly = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMNS.get("customer"), customerDomain));
        TupleDomain<IcebergColumnHandle> customerAndDay = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMNS.get("customer"), customerDomain,
                COLUMNS.get("day"), Domain.singleValue(DATE, (long) FIRST_DAY + days / 2)));
        Map<String, TupleDomain<IcebergColumnHandle>> filters = ImmutableMap.of(
                "customer = ?", customerOnly,
                "customer = ? AND day = ?", customerAndDay,
                "no filter", TupleDomain.all());

        report("load table (parse metadata.json)", "%s", time(3, () -> new HadoopTables().load(table.location())));

        for (Map.Entry<String, TupleDomain<IcebergColumnHandle>> filter : filters.entrySet()) {
            int[] files = new int[1];
            String timing = time(3, () -> files[0] = countFiles(scan(filter.getValue()).planFiles()));
            report("table scan: " + filter.getKey(), "%s, %,d files", timing, files[0]);
        }

        // At most two indexes are alive at any time, as on a coordinator: the one being built and the one it is built from
        long snapshotId = table.currentSnapshot().snapshotId();
        long heapBefore = usedHeap();
        SnapshotFileIndex index = timeBuild("index: first build", snapshotId, Optional.empty());
        long indexHeap = usedHeap() - heapBefore;
        report("index: heap", "%,d MB, %,d bytes per file", indexHeap >> 20, indexHeap / index.fileCount());
        for (Map.Entry<String, TupleDomain<IcebergColumnHandle>> filter : filters.entrySet()) {
            reportLookup("index lookup: " + filter.getKey(), index, filter.getValue());
        }

        timeBuild("index: rebuild, nothing changed", snapshotId, Optional.of(index));

        AppendFiles append = table.newAppend();
        for (int file = 0; file < filesPerDay; file++) {
            append.appendFile(dataFile(FIRST_DAY + days, file));
        }
        append.commit();
        index = timeBuild("index: build after appending a day", table.currentSnapshot().snapshotId(), Optional.of(index));

        table.newDelete()
                .deleteFromRowFilter(Expressions.equal("day", LocalDate.ofEpochDay(FIRST_DAY).toString()))
                .commit();
        index = timeBuild("index: build after dropping a day", table.currentSnapshot().snapshotId(), Optional.of(index));

        int[] files = new int[1];
        String timing = time(3, () -> files[0] = countFiles(scan(customerOnly).planFiles()));
        report("table scan after both commits: customer = ?", "%s, %,d files", timing, files[0]);
        reportLookup("index lookup after both commits: customer = ?", index, customerOnly);
    }

    /**
     * Builds the index once to warm up and then {@link #BUILD_RUNS} times, never keeping more than the last result.
     */
    private SnapshotFileIndex timeBuild(String name, long snapshotId, Optional<SnapshotFileIndex> previous)
    {
        List<Long> nanos = new ArrayList<>();
        SnapshotFileIndex index = null;
        for (int run = 0; run <= BUILD_RUNS; run++) {
            // let go of the previous run's result before building again
            index = null;
            long start = System.nanoTime();
            index = build(snapshotId, previous);
            if (run > 0) {
                nanos.add(System.nanoTime() - start);
            }
        }
        report(name, "%s", summarize(nanos));
        report(name + " read", "%s, %,d files", index.buildStatistics(), index.fileCount());
        return index;
    }

    private static void reportLookup(String name, SnapshotFileIndex index, TupleDomain<IcebergColumnHandle> predicate)
            throws Exception
    {
        Expression expression = toIcebergExpression(predicate);
        int[] files = new int[1];
        String timing = time(30, () -> files[0] = countFiles(index.planFiles(expression, predicate).tasks()));
        report(name, "%s, %,d files", timing, files[0]);
    }

    private SnapshotFileIndex build(long snapshotId, Optional<SnapshotFileIndex> previous)
    {
        return SnapshotFileIndex.build(table, snapshotId, INDEXED_COLUMNS, TESTING_TYPE_MANAGER, executor, MAX_FILES, previous).orElseThrow();
    }

    /**
     * The scan split generation runs for a query with the given filter: statistics are kept for the filtered data columns only.
     */
    private TableScan scan(TupleDomain<IcebergColumnHandle> predicate)
    {
        TableScan scan = table.newScan()
                .filter(toIcebergExpression(predicate))
                .planWith(executor);
        if (!predicate.isAll()) {
            scan = scan.includeColumnStats(INDEXED_COLUMNS);
        }
        return scan;
    }

    private static int countFiles(CloseableIterable<FileScanTask> tasks)
            throws Exception
    {
        int count = 0;
        try (tasks) {
            for (FileScanTask _ : tasks) {
                count++;
            }
        }
        return count;
    }

    private DataFile dataFile(int day, int file)
    {
        long firstCustomer = (long) file * CUSTOMERS_PER_FILE;
        long firstId = ((long) day * filesPerDay + file) * 1_000_000;
        long dayStartMicros = day * 86_400_000_000L;
        ImmutableMap.Builder<Integer, ByteBuffer> lowerBounds = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, ByteBuffer> upperBounds = ImmutableMap.builder();
        bounds(lowerBounds, upperBounds, 1, firstId, firstId + 999_999);
        bounds(lowerBounds, upperBounds, 2, customer(firstCustomer), customer(firstCustomer + CUSTOMERS_PER_FILE - 1));
        bounds(lowerBounds, upperBounds, 3, day, day);
        bounds(lowerBounds, upperBounds, 4, dayStartMicros, dayStartMicros + 86_399_999_999L);
        bounds(lowerBounds, upperBounds, 5, 0.01, 9_999.99);
        bounds(lowerBounds, upperBounds, 6, "CANCELLED", "SHIPPED");
        bounds(lowerBounds, upperBounds, 7, 1L, 500L);
        bounds(lowerBounds, upperBounds, 8, 0.5, 999.5);
        bounds(lowerBounds, upperBounds, 9, "ap-southeast-1", "us-west-2");
        bounds(lowerBounds, upperBounds, 10, "a note that is l", "zzz note that it");

        Map<Integer, Long> columnSizes = SCHEMA.columns().stream().collect(toImmutableMap(Types.NestedField::fieldId, _ -> 12_000_000L));
        Map<Integer, Long> valueCounts = SCHEMA.columns().stream().collect(toImmutableMap(Types.NestedField::fieldId, _ -> 1_000_000L));
        Map<Integer, Long> nullCounts = SCHEMA.columns().stream().collect(toImmutableMap(Types.NestedField::fieldId, _ -> 0L));
        return DataFiles.builder(SPEC)
                .withPath(format("%s/data/day=%s/20240101_000000_00000_abcde-%s.parquet", table.location(), LocalDate.ofEpochDay(day), UUID.randomUUID()))
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(128L << 20)
                .withPartition(new PartitionData(new Object[] {day}))
                .withMetrics(new Metrics(1_000_000L, columnSizes, valueCounts, nullCounts, null, lowerBounds.buildOrThrow(), upperBounds.buildOrThrow()))
                .build();
    }

    private static void bounds(ImmutableMap.Builder<Integer, ByteBuffer> lowerBounds, ImmutableMap.Builder<Integer, ByteBuffer> upperBounds, int fieldId, Object lower, Object upper)
    {
        lowerBounds.put(fieldId, Conversions.toByteBuffer(SCHEMA.findType(fieldId), lower));
        upperBounds.put(fieldId, Conversions.toByteBuffer(SCHEMA.findType(fieldId), upper));
    }

    private static String customer(long number)
    {
        return format("customer-%07d", number);
    }

    /**
     * Runs the task once to warm up, then the given number of times, and returns the median and the range of those runs.
     */
    private static String time(int runs, Callable<?> task)
            throws Exception
    {
        task.call();
        List<Long> nanos = new ArrayList<>();
        for (int run = 0; run < runs; run++) {
            long start = System.nanoTime();
            task.call();
            nanos.add(System.nanoTime() - start);
        }
        return summarize(nanos);
    }

    private static String summarize(List<Long> nanos)
    {
        List<Long> sorted = nanos.stream().sorted().toList();
        return format("median %s (min %s, max %s, %s runs)", seconds(sorted.get(sorted.size() / 2)), seconds(sorted.getFirst()), seconds(sorted.getLast()), sorted.size());
    }

    private static String seconds(long nanos)
    {
        if (nanos < 1_000_000_000) {
            return format(Locale.ENGLISH, "%.2f ms", nanos / 1e6);
        }
        return format(Locale.ENGLISH, "%.2f s", nanos / 1e9);
    }

    private static long usedHeap()
            throws InterruptedException
    {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(200);
        }
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void report(String name, String format, Object... arguments)
    {
        System.out.printf(Locale.ENGLISH, "%-50s %s%n", name, format(Locale.ENGLISH, format, arguments));
    }
}
