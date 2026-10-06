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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_POSITION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_SIZE;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.fileindex.SnapshotFileIndexManager.FILE_INDEX_COLUMNS;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.joining;

/**
 * Measures what a very large key list costs at the places it passes through: the per-split narrowing on a worker,
 * the file index lookup on the coordinator, and query planning.
 * <p>
 * Usage: {@code BenchmarkKeySetScaling split|index|server <output directory> [table directory] [pairs directory]}
 */
public final class BenchmarkKeySetScaling
{
    private static final int FILES_PER_DAY = 27_400;
    private static final IcebergColumnHandle KEY = IcebergColumnHandle.optional(new ColumnIdentity(1, "key", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()))
            .columnType(VARCHAR)
            .build();
    private static final IcebergColumnHandle DAY = IcebergColumnHandle.optional(new ColumnIdentity(2, "day", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()))
            .columnType(DATE)
            .build();

    private BenchmarkKeySetScaling() {}

    public static void main(String[] args)
            throws Exception
    {
        Path outputDirectory = Files.createDirectories(Path.of(args[1]).toAbsolutePath());
        try (PrintWriter report = new PrintWriter(Files.newBufferedWriter(outputDirectory.resolve(args[0] + ".txt"), UTF_8), true)) {
            switch (args[0]) {
                case "split" -> splitNarrowing(report);
                case "index" -> indexLookup(report, outputDirectory);
                case "server" -> server(report, outputDirectory, Path.of(args[2]).toAbsolutePath(), Path.of(args[3]).toAbsolutePath());
                default -> throw new IllegalArgumentException("Unknown mode: " + args[0]);
            }
        }
    }

    /**
     * What IcebergPageSourceProvider.getUnenforcedPredicate does for every split: intersect the query's predicate with
     * the file's statistics, drop what the statistics already imply, and compact.
     */
    private static void splitNarrowing(PrintWriter report)
    {
        Random random = new Random(1);
        // files with disjoint key ranges that together cover the key space, bounds truncated to 16 characters as Iceberg stores them
        List<TupleDomain<IcebergColumnHandle>> fileStatistics = new ArrayList<>();
        for (int file = 0; file < FILES_PER_DAY; file++) {
            Slice lower = Slices.utf8Slice(format("%016x", Long.divideUnsigned(-1L, FILES_PER_DAY) * file));
            Slice upper = Slices.utf8Slice(format("%016x", Long.divideUnsigned(-1L, FILES_PER_DAY) * (file + 1) - 1));
            fileStatistics.add(TupleDomain.withColumnDomains(ImmutableMap.of(KEY, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, lower, true, upper, true)), false))));
        }

        report.println("keys,retained_bytes_of_key_domain,nanos_per_split,keys_left_per_split");
        for (int keys : new int[] {1_000, 10_000, 27_400, 55_000, 110_000, 1_000_000}) {
            Domain keyDomain = Domain.multipleValues(VARCHAR, randomKeys(random, keys));
            TupleDomain<IcebergColumnHandle> unenforced = TupleDomain.withColumnDomains(ImmutableMap.of(KEY, keyDomain));
            long best = Long.MAX_VALUE;
            long keysLeft = 0;
            for (int round = 0; round < 12; round++) {
                keysLeft = 0;
                long start = System.nanoTime();
                for (TupleDomain<IcebergColumnHandle> statistics : fileStatistics) {
                    TupleDomain<IcebergColumnHandle> narrowed = TupleDomain.intersect(ImmutableList.of(unenforced, statistics, TupleDomain.<IcebergColumnHandle>all()));
                    if (!narrowed.isNone()) {
                        narrowed = narrowed
                                .filter((handle, domain) -> !domain.contains(statistics.getDomain(handle, domain.getType())))
                                .simplify(1000);
                        Domain left = narrowed.getDomains().orElseThrow().get(KEY);
                        if (left != null) {
                            keysLeft += left.getValues().getRanges().getRangeCount();
                        }
                    }
                }
                best = Math.min(best, System.nanoTime() - start);
            }
            report.println(format(Locale.ENGLISH, "%d,%d,%d,%.2f", keys, keyDomain.getRetainedSizeInBytes(), best / FILES_PER_DAY, (double) keysLeft / FILES_PER_DAY));
        }
    }

    /**
     * The file index lookup for one day-query: a day and 27,400 keys, against tables of 27,400 files per day and a growing number of days.
     */
    private static void indexLookup(PrintWriter report, Path outputDirectory)
            throws Exception
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "key", Types.StringType.get()),
                Types.NestedField.optional(2, "day", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("day").build();
        ExecutorService executor = newFixedThreadPool(16, daemonThreadsNamed("benchmark-%s"));
        int firstDay = 20_000;
        report.println("days,files,keys,first_build_ms,lookup_ms_median,files_returned,partition_candidates,column_candidates");
        for (int days : new int[] {1, 10, 30, 100}) {
            Table table = new HadoopTables().create(schema, spec, ImmutableMap.of("format-version", "2"), outputDirectory.resolve("index-table-" + days).toString());
            for (int day = 0; day < days; day += 10) {
                AppendFiles append = table.newAppend();
                for (int commitDay = day; commitDay < Math.min(day + 10, days); commitDay++) {
                    for (int file = 0; file < FILES_PER_DAY; file++) {
                        // every day has the same layout: file n holds the n-th slice of the key space
                        ByteBuffer lower = Conversions.toByteBuffer(Types.StringType.get(), format("%016x", Long.divideUnsigned(-1L, FILES_PER_DAY) * file));
                        ByteBuffer upper = Conversions.toByteBuffer(Types.StringType.get(), format("%016x", Long.divideUnsigned(-1L, FILES_PER_DAY) * (file + 1) - 1));
                        append.appendFile(DataFiles.builder(spec)
                                .withPath(format("%s/data/day=%s/%s.parquet", table.location(), LocalDate.ofEpochDay(firstDay + commitDay), UUID.randomUUID()))
                                .withFormat(FileFormat.PARQUET)
                                .withFileSizeInBytes(512L << 20)
                                .withPartition(new PartitionData(new Object[] {firstDay + commitDay}))
                                .withMetrics(new Metrics(1_000_000L, null, ImmutableMap.of(1, 1_000_000L), ImmutableMap.of(1, 0L), null, ImmutableMap.of(1, lower), ImmutableMap.of(1, upper)))
                                .build());
                    }
                }
                append.commit();
            }
            long snapshotId = table.currentSnapshot().snapshotId();
            long start = System.nanoTime();
            SnapshotFileIndex index = SnapshotFileIndex.build(table, snapshotId, ImmutableList.of("key"), TESTING_TYPE_MANAGER, executor, 100_000_000, Optional.empty()).orElseThrow();
            double buildMillis = (System.nanoTime() - start) / 1e6;

            for (int keys : new int[] {1, 274, 2_740, 27_400}) {
                TupleDomain<IcebergColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                        KEY, Domain.multipleValues(VARCHAR, randomKeys(new Random(keys), keys)),
                        DAY, Domain.singleValue(DATE, (long) firstDay + days / 2)));
                Expression expression = toIcebergExpression(predicate);
                List<Double> millis = new ArrayList<>();
                int returned = 0;
                SnapshotFileIndex.FileIndexScan scan = null;
                for (int run = 0; run < 12; run++) {
                    long lookupStart = System.nanoTime();
                    scan = index.planFiles(expression, predicate);
                    returned = 0;
                    try (CloseableIterable<FileScanTask> tasks = scan.tasks()) {
                        for (FileScanTask _ : tasks) {
                            returned++;
                        }
                    }
                    if (run >= 2) {
                        millis.add((System.nanoTime() - lookupStart) / 1e6);
                    }
                }
                millis.sort(Comparator.naturalOrder());
                report.println(format(Locale.ENGLISH, "%d,%d,%d,%.0f,%.2f,%d,%d,%d", days, index.fileCount(), keys, buildMillis, millis.get(millis.size() / 2), returned, scan.partitionCandidates(), scan.columnCandidates()));
            }
        }
        executor.shutdownNow();
    }

    private static void server(PrintWriter report, Path outputDirectory, Path tableDirectory, Path pairsDirectory)
            throws Exception
    {
        String version = Files.readString(tableDirectory.resolve("metadata/version-hint.text")).trim();
        String metadataFileName = "v" + version + ".metadata.json";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metadata = (ObjectNode) mapper.readTree(tableDirectory.resolve("metadata").resolve(metadataFileName).toFile());
        String tableLocation = metadata.get("location").asText();
        long snapshotId = metadata.get("refs").get("fresh0").get("snapshot-id").asLong();
        Path indexedMetadataDirectory = Files.createDirectories(outputDirectory.resolve("indexed-metadata"));
        ((ObjectNode) metadata.get("properties")).put(FILE_INDEX_COLUMNS, "key");
        mapper.writeValue(indexedMetadataDirectory.resolve(metadataFileName).toFile(), metadata);

        Random random = new Random(5);
        List<String> present = new ArrayList<>();
        for (String line : Files.readAllLines(pairsDirectory.resolve("fresh0-28000-present.csv"))) {
            if (line.startsWith("2026-01-01,")) {
                present.add(line.substring(line.indexOf(',') + 1));
            }
        }
        present.sort(Comparator.naturalOrder());
        List<String> hexKeys27 = randomKeys(random, 27_400).stream().map(Slice::toStringUtf8).toList();
        List<String> hexKeys55 = randomKeys(random, 55_000).stream().map(Slice::toStringUtf8).toList();
        List<String> uuidKeys27 = new ArrayList<>();
        for (int i = 0; i < 27_400; i++) {
            uuidKeys27.add(new UUID(random.nextLong(), random.nextLong()).toString());
        }

        // 1. the query length limit, on a server with default settings
        try (DistributedQueryRunner queryRunner = startServer(ImmutableMap.of(), tableLocation, metadataFileName, indexedMetadataDirectory)) {
            report.println("== query length, default query.max-length ==");
            for (Map.Entry<String, List<String>> keys : ImmutableMap.of("27,400 keys of 32 characters", hexKeys27, "27,400 keys of 36 characters (UUID with dashes)", uuidKeys27, "55,000 keys of 32 characters", hexKeys55).entrySet()) {
                String sql = "EXPLAIN " + lookupQuery("iceberg", "events_indexed", snapshotId, keys.getValue());
                String outcome = "accepted";
                try {
                    queryRunner.execute(sql);
                }
                catch (RuntimeException e) {
                    outcome = "rejected: " + e.getMessage();
                }
                report.println(format("%s: query text %,d characters, %s", keys.getKey(), sql.length(), outcome));
            }
        }

        try (DistributedQueryRunner queryRunner = startServer(ImmutableMap.of("query.max-length", "100000000"), tableLocation, metadataFileName, indexedMetadataDirectory)) {
            Session session = queryRunner.getDefaultSession();
            // the index of a snapshot is built in the background by the first query
            for (String catalog : ImmutableList.of("iceberg", "iceberg_footer_cache")) {
                String probe = lookupQuery(catalog, "events_indexed", snapshotId, present.subList(0, 1));
                while (run(queryRunner, session, probe).manifestReads() > 0) {
                    Thread.sleep(50);
                }
            }

            // 2. one key in one file: every read of the data file, in the order made
            report.println();
            report.println("== reads for one key in one 500 MB file (prototype on) ==");
            List<String> oneKey = present.subList(7_000, 7_001);
            Run single = run(queryRunner, session, lookupQuery("iceberg", "events_indexed", snapshotId, oneKey));
            report.println(format("splits %d, data files %d, reads %d, bytes %,d", single.splits(), single.dataFiles(), single.reads().size(), single.dataBytes()));
            for (long[] read : single.reads()) {
                report.println(format("  %s at %,d, %,d bytes", read[3] == 1 ? "tail" : "read", read[1], read[2]));
            }
            Run cached = run(queryRunner, session, lookupQuery("iceberg_footer_cache", "events_indexed", snapshotId, oneKey));
            cached = run(queryRunner, session, lookupQuery("iceberg_footer_cache", "events_indexed", snapshotId, oneKey));
            report.println(format("with iceberg.parquet-footer-cache.type=memory, second run: splits %d, reads %d, bytes %,d", cached.splits(), cached.reads().size(), cached.dataBytes()));
            Session oneSplitPerFile = Session.builder(session)
                    .setCatalogSessionProperty("iceberg", IcebergSessionProperties.SPLIT_SIZE, "1GB")
                    .build();
            Run merged = run(queryRunner, oneSplitPerFile, lookupQuery("iceberg", "events_indexed", snapshotId, oneKey));
            report.println(format("with session property %s=1GB: splits %d, reads %d, bytes %,d, rows %d", IcebergSessionProperties.SPLIT_SIZE, merged.splits(), merged.reads().size(), merged.dataBytes(), merged.rows()));
            for (long[] read : merged.reads()) {
                report.println(format("  %s at %,d, %,d bytes", read[3] == 1 ? "tail" : "read", read[1], read[2]));
            }
            Run mergedMany = run(queryRunner, oneSplitPerFile, lookupQuery("iceberg", "events_indexed", snapshotId, present));
            report.println(format(Locale.ENGLISH, "with %s=1GB, %,d keys spread over the day: splits %d, reads %d, data MB %.1f, rows %d, wall %.0f ms", IcebergSessionProperties.SPLIT_SIZE, present.size(), mergedMany.splits(), mergedMany.reads().size(), mergedMany.dataBytes() / 1e6, mergedMany.rows(), mergedMany.wallMillis()));

            // 3. splits with and without the prototype
            report.println();
            report.println("== splits, files and reads per query: prototype off / on ==");
            Map<String, List<String>> keySets = new HashMap<>();
            keySets.put("1 key", oneKey);
            keySets.put("300 keys, all in the first file's key range", present.subList(0, 300));
            keySets.put("all keys of the day, spread over its files", present);
            for (String name : ImmutableList.of("1 key", "300 keys, all in the first file's key range", "all keys of the day, spread over its files")) {
                for (String table : ImmutableList.of("events_plain", "events_indexed")) {
                    String sql = lookupQuery("iceberg", table, snapshotId, keySets.get(name));
                    run(queryRunner, session, sql);
                    Run result = run(queryRunner, session, sql);
                    report.println(format(
                            Locale.ENGLISH,
                            "%s, %s: splits %d, data files opened %d, reads %d, data MB %.1f, manifest reads %d, rows %d, wall %.0f ms, cpu %.0f ms",
                            name,
                            table,
                            result.splits(),
                            result.dataFiles(),
                            result.reads().size(),
                            result.dataBytes() / 1e6,
                            result.manifestReads(),
                            result.rows(),
                            result.wallMillis(),
                            result.cpuMillis()));
                }
            }

            // 4. planning cost of long lists (the keys are random, so nothing matches)
            report.println();
            report.println("== planning: EXPLAIN only, then the query itself (prototype on) ==");
            for (Map.Entry<String, List<String>> keys : ImmutableMap.of("1,400 keys", present.subList(0, 1_400), "all " + present.size() + " keys of the day", present, "27,400 keys", hexKeys27, "55,000 keys", hexKeys55).entrySet()) {
                String sql = lookupQuery("iceberg", "events_indexed", snapshotId, keys.getValue());
                double[] explainMillis = new double[5];
                long[] explainAllocated = new long[5];
                for (int i = 0; i < 7; i++) {
                    long allocatedBefore = allocatedBytes();
                    long start = System.nanoTime();
                    queryRunner.execute(session, "EXPLAIN " + sql);
                    if (i >= 2) {
                        explainMillis[i - 2] = (System.nanoTime() - start) / 1e6;
                        explainAllocated[i - 2] = allocatedBytes() - allocatedBefore;
                    }
                }
                Arrays.sort(explainMillis);
                Arrays.sort(explainAllocated);
                run(queryRunner, session, sql);
                Run result = run(queryRunner, session, sql);
                report.println(format(
                        Locale.ENGLISH,
                        "%s: query text %,d characters; EXPLAIN %.0f ms and about %.0f MB allocated; query: planning %.0f ms, wall %.0f ms, cpu %.0f ms, splits %d, data files opened %d, data MB %.1f, rows %d",
                        keys.getKey(),
                        sql.length(),
                        explainMillis[2],
                        explainAllocated[2] / 1e6,
                        result.planningMillis(),
                        result.wallMillis(),
                        result.cpuMillis(),
                        result.splits(),
                        result.dataFiles(),
                        result.dataBytes() / 1e6,
                        result.rows()));
            }
        }
    }

    private static DistributedQueryRunner startServer(Map<String, String> coordinatorProperties, String tableLocation, String metadataFileName, Path indexedMetadataDirectory)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("poc")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .setCoordinatorProperties(coordinatorProperties)
                .build();
        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        Files.createDirectories(dataDirectory);
        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        Map<String, String> catalogProperties = ImmutableMap.of(
                "fs.hadoop.enabled", "true",
                "iceberg.register-table-procedure.enabled", "true",
                "iceberg.file-index.enabled", "true");
        queryRunner.createCatalog("iceberg", "iceberg", catalogProperties);
        queryRunner.createCatalog("iceberg_footer_cache", "iceberg", ImmutableMap.<String, String>builder()
                .putAll(catalogProperties)
                .put("iceberg.parquet-footer-cache.type", "memory")
                .buildOrThrow());
        for (String catalog : ImmutableList.of("iceberg", "iceberg_footer_cache")) {
            queryRunner.execute(format("CREATE SCHEMA %s.poc", catalog));
            queryRunner.execute(format(
                    "CALL %s.system.register_table(schema_name => 'poc', table_name => 'events_plain', table_location => '%s', metadata_file_name => '%s')",
                    catalog,
                    tableLocation,
                    metadataFileName));
            queryRunner.execute(format(
                    "CALL %s.system.register_table(schema_name => 'poc', table_name => 'events_indexed', table_location => '%s', metadata_file_name => '%s', metadata_location => '%s')",
                    catalog,
                    tableLocation,
                    metadataFileName,
                    indexedMetadataDirectory.toUri()));
        }
        return queryRunner;
    }

    private static String lookupQuery(String catalog, String table, long snapshotId, List<String> keys)
    {
        return format(
                "SELECT key, day, id, amount, payload FROM %s.poc.%s FOR VERSION AS OF %s WHERE day = DATE '2026-01-01' AND key IN (%s)",
                catalog,
                table,
                snapshotId,
                keys.stream().map(key -> "'" + key + "'").collect(joining(", ")));
    }

    private static Run run(DistributedQueryRunner queryRunner, Session session, String sql)
    {
        long start = System.nanoTime();
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, sql);
        double wallMillis = (System.nanoTime() - start) / 1e6;
        List<SpanData> spans = queryRunner.getSpans();
        QueryInfo queryInfo = queryRunner.getCoordinator().getFullQueryInfo(result.queryId());
        QueryStats stats = queryInfo.getQueryStats();

        List<long[]> reads = new ArrayList<>();
        Map<String, Boolean> dataFiles = new HashMap<>();
        long manifestReads = 0;
        long dataBytes = 0;
        for (SpanData span : spans) {
            String location = span.getAttributes().get(FILE_LOCATION);
            if (location == null) {
                continue;
            }
            if (span.getName().equals("InputFile.newStream") && location.endsWith(".avro") && !location.substring(location.lastIndexOf('/') + 1).startsWith("snap-")) {
                manifestReads++;
            }
            if (!location.endsWith(".parquet") || (!span.getName().equals("Input.readFully") && !span.getName().equals("Input.readTail"))) {
                continue;
            }
            long size = span.getAttributes().get(FILE_READ_SIZE);
            Long position = span.getAttributes().get(FILE_READ_POSITION);
            reads.add(new long[] {span.getStartEpochNanos(), position == null ? -1 : position, size, position == null ? 1 : 0});
            dataFiles.put(location, true);
            dataBytes += size;
        }
        reads.sort(Comparator.comparingLong((long[] read) -> read[0]));

        long splits = 0;
        for (OperatorStats operator : stats.getOperatorSummaries()) {
            if (operator.getOperatorType().contains("Scan")) {
                splits += operator.getTotalDrivers();
            }
        }
        return new Run(result.result().getRowCount(), wallMillis, stats.getPlanningTime().toMillis(), stats.getTotalCpuTime().toMillis(), splits, dataFiles.size(), reads, dataBytes, manifestReads);
    }

    private static long allocatedBytes()
    {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long total = 0;
        for (long allocated : threads.getThreadAllocatedBytes(threads.getAllThreadIds())) {
            if (allocated > 0) {
                total += allocated;
            }
        }
        return total;
    }

    private static List<Slice> randomKeys(Random random, int count)
    {
        List<Slice> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(Slices.utf8Slice(format("%016x%016x", random.nextLong(), random.nextLong())));
        }
        return keys;
    }

    private record Run(long rows, double wallMillis, double planningMillis, double cpuMillis, long splits, int dataFiles, List<long[]> reads, long dataBytes, long manifestReads) {}
}
