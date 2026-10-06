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
import com.sun.management.OperatingSystemMXBean;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_POSITION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_SIZE;
import static io.trino.plugin.iceberg.fileindex.SnapshotFileIndexManager.FILE_INDEX_COLUMNS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tracing.TrinoAttributes.PIPELINE_ID;
import static io.trino.tracing.TrinoAttributes.SPLIT_CPU_TIME_NANOS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

/**
 * Looks up sets of (day, key) pairs in an existing Iceberg table with one query per day, with and without the
 * file index, and records time, CPU, splits, and what was read from the data files.
 * <p>
 * The table is only read. It is registered in a file metastore that lives in the output directory. For the
 * file index, a copy of the table's metadata file with the {@code trino.file-index.columns} property added is
 * written to the output directory and registered from there, so nothing is written under the table location.
 * <p>
 * Usage: {@code BenchmarkFileIndexLookup <table directory> <pairs directory> <output directory> large|day [case name filter]}
 * <ul>
 * <li>{@code large}: few large files; compares reader settings</li>
 * <li>{@code day}: many small files; compares split handling, and adds two cases whose keys all lie in one file</li>
 * </ul>
 */
public final class BenchmarkFileIndexLookup
{
    private static final String COLUMNS = "key, day, id, amount, payload";
    private static final int MEASURED_RUNS = 3;
    private static final long WIDE_COMPACTION_THRESHOLD = 1_000_000;
    private static final List<String> CATALOGS = ImmutableList.of("iceberg", "iceberg_wide", "iceberg_nomerge", "iceberg_footer_cache");
    // Optional: more nodes in the same JVM, and only some of the variants, separated by "|"
    private static final int WORKERS = Integer.getInteger("benchmark.workers", 0);
    private static final List<String> VARIANT_FILTER = ImmutableList.copyOf(System.getProperty("benchmark.variants", "").split("\\|"));

    private final DistributedQueryRunner queryRunner;
    private final Map<String, Long> snapshotIds;
    private final Map<String, Map<String, Long>> unsortedFiles;
    private final boolean manyFiles;
    private final PrintWriter results;
    private final PrintWriter threadResults;

    private BenchmarkFileIndexLookup(
            DistributedQueryRunner queryRunner,
            Map<String, Long> snapshotIds,
            Map<String, Map<String, Long>> unsortedFiles,
            boolean manyFiles,
            PrintWriter results,
            PrintWriter threadResults)
    {
        this.queryRunner = queryRunner;
        this.snapshotIds = snapshotIds;
        this.unsortedFiles = unsortedFiles;
        this.manyFiles = manyFiles;
        this.results = results;
        this.threadResults = threadResults;
    }

    public static void main(String[] args)
            throws Exception
    {
        Path tableDirectory = Path.of(args[0]).toAbsolutePath();
        Path pairsDirectory = Path.of(args[1]).toAbsolutePath();
        Path outputDirectory = Files.createDirectories(Path.of(args[2]).toAbsolutePath());
        boolean manyFiles = args[3].equals("day");
        Optional<String> caseFilter = Optional.empty();
        if (args.length > 4) {
            caseFilter = Optional.of(args[4]);
        }

        String version = Files.readString(tableDirectory.resolve("metadata/version-hint.text")).trim();
        String metadataFileName = "v" + version + ".metadata.json";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metadata = (ObjectNode) mapper.readTree(tableDirectory.resolve("metadata").resolve(metadataFileName).toFile());
        String tableLocation = metadata.get("location").asText();
        Map<String, Long> snapshotIds = new HashMap<>();
        metadata.get("refs").properties().forEach(ref -> snapshotIds.put(ref.getKey(), ref.getValue().get("snapshot-id").asLong()));

        // A copy of the metadata that asks for the file index. It lives outside the table directory.
        Path indexedMetadataDirectory = Files.createDirectories(outputDirectory.resolve("indexed-metadata"));
        ((ObjectNode) metadata.get("properties")).put(FILE_INDEX_COLUMNS, "key");
        mapper.writeValue(indexedMetadataDirectory.resolve(metadataFileName).toFile(), metadata);

        // The files each tag has beyond the sorted ones of fresh0, by file name, with their sizes
        Table table = new HadoopTables().load(tableDirectory.toString());
        Set<String> sortedFiles = fileSizes(table, snapshotIds.get("fresh0")).keySet();
        Map<String, Map<String, Long>> unsortedFiles = new HashMap<>();
        for (Map.Entry<String, Long> tag : snapshotIds.entrySet()) {
            Map<String, Long> files = fileSizes(table, tag.getValue());
            files.keySet().removeAll(sortedFiles);
            unsortedFiles.put(tag.getKey(), files);
        }

        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("poc")
                .build();
        // one node, like Spark's local mode: the coordinator also runs the splits
        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setWorkerCount(WORKERS).build();
                PrintWriter results = new PrintWriter(Files.newBufferedWriter(outputDirectory.resolve("trino-lookup.csv"), UTF_8));
                PrintWriter threadResults = new PrintWriter(Files.newBufferedWriter(outputDirectory.resolve("trino-lookup-threads.csv"), UTF_8))) {
            Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
            Files.createDirectories(dataDirectory);
            queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
            Map<String, String> catalogProperties = ImmutableMap.of(
                    "fs.hadoop.enabled", "true",
                    "iceberg.register-table-procedure.enabled", "true",
                    "iceberg.file-index.enabled", "true");
            queryRunner.createCatalog("iceberg", "iceberg", catalogProperties);
            // the same, except that large value lists are not reduced to a range before they reach the Parquet reader
            queryRunner.createCatalog("iceberg_wide", "iceberg", ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("iceberg.domain-compaction-threshold", Long.toString(WIDE_COMPACTION_THRESHOLD))
                    .buildOrThrow());
            // the same as the first, except that the Parquet reader does not read through the gap between two nearby ranges
            queryRunner.createCatalog("iceberg_nomerge", "iceberg", ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("parquet.max-merge-distance", "0B")
                    .buildOrThrow());
            // the same as the first, except that parsed Parquet footers are kept in memory, with room for all of them
            queryRunner.createCatalog("iceberg_footer_cache", "iceberg", ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("iceberg.parquet-footer-cache.type", "memory")
                    .put("iceberg.parquet-footer-cache.memory.max-size", "2GB")
                    .buildOrThrow());
            for (String catalog : CATALOGS) {
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

            results.println("case,tag,mix,pairs,variant,runs,day,keys,rows,wall_ms,elapsed_ms,planning_ms,cpu_ms,planning_cpu_ms,process_cpu_ms,"
                    + "split_source_ms,split_batch_ms,splits,splits_with_reads,splits_without_reads,splits_without_reads_ms,splits_without_reads_cpu_ms,splits_with_reads_ms,splits_with_reads_cpu_ms,"
                    + "footer_reads,data_bytes,ranges,ranges_in_read_order,data_files_opened,data_reads,physical_input_bytes,physical_input_rows,"
                    + "unsorted_files,unsorted_file_bytes,unsorted_files_opened,unsorted_reads,unsorted_bytes_read,"
                    + "manifest_reads,index_used,index_files,index_partition_candidates,index_column_candidates,matches_reference");
            threadResults.println("case,variant,threads,cpu_ms");
            BenchmarkFileIndexLookup benchmark = new BenchmarkFileIndexLookup(queryRunner, snapshotIds, unsortedFiles, manyFiles, results, threadResults);

            List<LookupCase> cases = new ArrayList<>();
            List<String> caseLines = Files.readAllLines(pairsDirectory.resolve("index.csv"));
            for (String line : caseLines.subList(1, caseLines.size())) {
                String[] fields = line.split(",");
                cases.add(new LookupCase(fields[0].replace(".csv", ""), fields[1], fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), readPairs(pairsDirectory.resolve(fields[0]))));
            }
            if (manyFiles) {
                cases.addAll(1, oneFileCases(cases.getFirst()));
            }

            // The whole series runs twice, so that no recorded case pays for the JVM warming up. The first pass runs
            // each query once and is recorded only where it shows a cache that is still empty.
            for (boolean record : new boolean[] {false, true}) {
                String warmedTag = null;
                for (LookupCase lookupCase : cases) {
                    if (caseFilter.isPresent() && !lookupCase.name().contains(caseFilter.get())) {
                        continue;
                    }
                    if (!lookupCase.tag().equals(warmedTag)) {
                        for (String catalog : CATALOGS) {
                            benchmark.waitForIndex(catalog, lookupCase);
                        }
                        warmedTag = lookupCase.tag();
                    }
                    benchmark.run(lookupCase, record);
                    results.flush();
                    threadResults.flush();
                }
            }
        }
    }

    /**
     * Two cases whose keys all fall inside the key range of a single file: one short enough for Iceberg to
     * prune files with at planning, one too long for that. Only the first key exists.
     */
    private static List<LookupCase> oneFileCases(LookupCase source)
    {
        String[] middle = source.dayAndKey().get(source.dayAndKey().size() / 2);
        ImmutableList.Builder<LookupCase> cases = ImmutableList.builder();
        for (int keys : new int[] {150, 300}) {
            List<String[]> pairs = new ArrayList<>();
            pairs.add(middle);
            for (int i = 1; i < keys; i++) {
                // sorts directly after the existing key, and before every other key of the table
                pairs.add(new String[] {middle[0], middle[1] + format("%04d", i)});
            }
            cases.add(new LookupCase(format("%s-one-file-%d-keys", source.tag(), keys), source.tag(), "one file", keys, 1, pairs));
        }
        return cases.build();
    }

    private void run(LookupCase lookupCase, boolean record)
    {
        Session session = queryRunner.getDefaultSession();
        List<Variant> variants = new ArrayList<>();
        variants.add(new Variant("prototype off", "iceberg", "events_plain", session));
        variants.add(new Variant("prototype on", "iceberg", "events_indexed", session));
        // on the many-files table the threshold only matters for the unsorted files, which fresh0 does not have
        if (!manyFiles || !unsortedFiles.get(lookupCase.tag()).isEmpty()) {
            variants.add(new Variant("prototype on + domain-compaction-threshold=" + WIDE_COMPACTION_THRESHOLD, "iceberg_wide", "events_indexed", session));
            variants.add(new Variant("prototype off + domain-compaction-threshold=" + WIDE_COMPACTION_THRESHOLD, "iceberg_wide", "events_plain", session));
        }
        if (manyFiles) {
            Session oneSplitPerFile = Session.builder(session)
                    .setCatalogSessionProperty("iceberg", IcebergSessionProperties.SPLIT_SIZE, "1GB")
                    .build();
            variants.add(new Variant("prototype off + max_split_size=1GB", "iceberg", "events_plain", oneSplitPerFile));
            variants.add(new Variant("prototype on + max_split_size=1GB", "iceberg", "events_indexed", oneSplitPerFile));
            variants.add(new Variant("prototype off + footer cache", "iceberg_footer_cache", "events_plain", session));
            variants.add(new Variant("prototype on + footer cache", "iceberg_footer_cache", "events_indexed", session));
        }
        else {
            variants.add(new Variant("prototype on + max-merge-distance=0B", "iceberg_nomerge", "events_indexed", session));
            if (lookupCase.pairs() == 280) {
                variants.add(new Variant("prototype on + bloom filter off", "iceberg", "events_indexed", Session.builder(session)
                        .setCatalogSessionProperty("iceberg", "parquet_use_bloom_filter", "false")
                        .build()));
                variants.add(new Variant("prototype on + page index off", "iceberg", "events_indexed", Session.builder(session)
                        .setCatalogSessionProperty("iceberg", "parquet_use_column_index", "false")
                        .build()));
            }
        }
        if (!VARIANT_FILTER.equals(ImmutableList.of(""))) {
            variants.removeIf(variant -> !VARIANT_FILTER.contains(variant.name()));
        }

        Map<String, Set<List<Object>>> reference = new HashMap<>();
        for (Variant variant : variants) {
            Measurement total = null;
            Map<String, Double> threadCpuMillis = new HashMap<>();
            long totalRows = 0;
            boolean allMatch = true;
            for (Map.Entry<String, List<String>> day : lookupCase.keysByDay().entrySet()) {
                String sql = lookupQuery(variant, lookupCase, day.getKey(), day.getValue());
                List<Measurement> runs = new ArrayList<>();
                Set<List<Object>> rows = null;
                for (int run = 0; run <= MEASURED_RUNS; run++) {
                    Execution execution = execute(variant.session(), sql, lookupCase.tag());
                    if (run > 0) {
                        runs.add(execution.measurement());
                        execution.threadCpuMillis().forEach((threads, millis) -> threadCpuMillis.merge(threads, millis / MEASURED_RUNS, Double::sum));
                    }
                    if (rows != null && !rows.equals(execution.rows())) {
                        throw new IllegalStateException("Result changed between runs of " + lookupCase.name() + " / " + variant.name());
                    }
                    rows = execution.rows();
                    if (!record) {
                        // The first pass over the cases: one run per query
                        runs.add(execution.measurement());
                        break;
                    }
                }
                // "prototype off" runs first and is the reference for the others
                reference.putIfAbsent(day.getKey(), rows);
                boolean matches = rows.equals(reference.get(day.getKey()));
                allMatch &= matches;
                Measurement median = Measurement.median(runs);
                if (record) {
                    write(lookupCase, variant, "median of 3", day.getKey(), day.getValue().size(), median, matches);
                }
                else if (variant.catalog().contains("footer_cache")) {
                    write(lookupCase, variant, "first pass", day.getKey(), day.getValue().size(), median, matches);
                }
                total = total == null ? median : total.plus(median);
                totalRows += rows.size();
            }
            if (totalRows != lookupCase.expectedRows() || !allMatch) {
                throw new IllegalStateException(format("%s / %s returned %s rows, expected %s, matches reference: %s", lookupCase.name(), variant.name(), totalRows, lookupCase.expectedRows(), allMatch));
            }
            if (!record) {
                continue;
            }
            if (lookupCase.keysByDay().size() > 1) {
                write(lookupCase, variant, "median of 3", "all", lookupCase.pairs(), total, allMatch);
            }
            // the mean of the measured runs, largest first
            threadCpuMillis.entrySet().stream()
                    .filter(entry -> entry.getValue() >= 0.5)
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .forEach(entry -> threadResults.println(format(Locale.ENGLISH, "%s,%s,%s,%.1f", lookupCase.name(), variant.name(), entry.getKey(), entry.getValue())));
            System.out.println(format(
                    Locale.ENGLISH,
                    "%-28s %-62s rows=%-6d wall=%7.1f ms cpu=%8.1f ms process cpu=%8.1f ms splits=%-6d without reads=%-6d files=%-5d reads=%-7d index=%s",
                    lookupCase.name(),
                    variant.name(),
                    total.rows(),
                    total.wallMillis(),
                    total.cpuMillis(),
                    total.processCpuMillis(),
                    total.splits(),
                    total.splitsWithoutReads(),
                    total.dataFilesOpened(),
                    total.dataReads(),
                    total.indexUsed()));
        }
    }

    private String lookupQuery(Variant variant, LookupCase lookupCase, String day, List<String> keys)
    {
        return format(
                "SELECT %s FROM %s.poc.%s FOR VERSION AS OF %s WHERE day = DATE '%s' AND key IN (%s)",
                COLUMNS,
                variant.catalog(),
                variant.table(),
                snapshotIds.get(lookupCase.tag()),
                day,
                keys.stream().map(key -> "'" + key + "'").collect(joining(", ")));
    }

    /**
     * The index of a snapshot is built in the background when the snapshot is first queried. Repeats a small
     * query until split generation reports that it used the index.
     */
    private void waitForIndex(String catalog, LookupCase lookupCase)
            throws InterruptedException
    {
        Map.Entry<String, List<String>> day = lookupCase.keysByDay().entrySet().iterator().next();
        // a key that sorts before every key of the table, so that no file is opened and no footer is cached
        String sql = lookupQuery(new Variant("warm-up", catalog, "events_indexed", queryRunner.getDefaultSession()), lookupCase, day.getKey(), ImmutableList.of(""));
        long start = System.nanoTime();
        int queries = 1;
        while (!execute(queryRunner.getDefaultSession(), sql, lookupCase.tag()).measurement().indexUsed()) {
            if (System.nanoTime() - start > 120_000_000_000L) {
                throw new IllegalStateException("File index for " + lookupCase.tag() + " in " + catalog + " was not ready after two minutes");
            }
            Thread.sleep(20);
            queries++;
        }
        System.out.println(format(Locale.ENGLISH, "index for %s in %s used after %.0f ms and %d queries", lookupCase.tag(), catalog, (System.nanoTime() - start) / 1e6, queries));
    }

    private Execution execute(Session session, String sql, String tag)
    {
        OperatingSystemMXBean operatingSystem = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        Map<Long, Long> startThreadCpu = threadCpuNanos();
        long startCpu = operatingSystem.getProcessCpuTime();
        long start = System.nanoTime();
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, sql);
        double wallMillis = (System.nanoTime() - start) / 1e6;
        double processCpuMillis = (operatingSystem.getProcessCpuTime() - startCpu) / 1e6;
        Map<String, Double> threadCpuMillis = threadCpuMillis(startThreadCpu);
        List<SpanData> spans = queryRunner.getSpans();
        QueryInfo queryInfo = queryRunner.getCoordinator().getFullQueryInfo(result.queryId());
        QueryStats stats = queryInfo.getQueryStats();

        Map<String, SpanData> spansById = new HashMap<>();
        for (SpanData span : spans) {
            spansById.put(span.getSpanId(), span);
        }

        Map<String, Long> unsorted = unsortedFiles.getOrDefault(tag, ImmutableMap.of());
        Map<String, List<long[]>> readsByFile = new HashMap<>();
        Set<String> splitsWithReads = new HashSet<>();
        long manifestReads = 0;
        long footerReads = 0;
        double splitSourceMillis = 0;
        double splitBatchMillis = 0;
        for (SpanData span : spans) {
            // the split source lives from the start of split generation until the last split was handed to the scheduler
            if (span.getName().equals("split-source")) {
                splitSourceMillis += (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1e6;
            }
            // the time the scheduler waited for the connector to produce the next batch of splits
            if (span.getName().equals("split-buffer")) {
                splitBatchMillis += (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1e6;
            }
            String location = span.getAttributes().get(FILE_LOCATION);
            if (location == null) {
                continue;
            }
            // manifests are Avro files read as streams; files named snap-* are manifest lists
            if (span.getName().equals("InputFile.newStream") && location.endsWith(".avro") && !location.substring(location.lastIndexOf('/') + 1).startsWith("snap-")) {
                manifestReads++;
            }
            if (!location.endsWith(".parquet") || (!span.getName().equals("Input.readFully") && !span.getName().equals("Input.readTail"))) {
                continue;
            }
            long size = span.getAttributes().get(FILE_READ_SIZE);
            Long position = span.getAttributes().get(FILE_READ_POSITION);
            if (position == null) {
                footerReads++;
            }
            // a tail read has no position: it is the last bytes of the file, which no other read here ends at
            readsByFile.computeIfAbsent(location, _ -> new ArrayList<>())
                    .add(new long[] {span.getStartEpochNanos(), position == null ? Long.MAX_VALUE - size : position, size});
            // the split that did the read
            SpanData ancestor = span;
            while (ancestor != null && !ancestor.getName().equals("split")) {
                ancestor = spansById.get(ancestor.getParentSpanId());
            }
            if (ancestor != null) {
                splitsWithReads.add(ancestor.getSpanId());
            }
        }
        long dataBytes = 0;
        long dataReads = 0;
        long ranges = 0;
        long rangesInReadOrder = 0;
        long unsortedFilesOpened = 0;
        long unsortedReads = 0;
        long unsortedBytesRead = 0;
        for (Map.Entry<String, List<long[]>> file : readsByFile.entrySet()) {
            List<long[]> reads = file.getValue();
            dataReads += reads.size();
            boolean isUnsorted = unsorted.containsKey(file.getKey().substring(file.getKey().lastIndexOf('/') + 1));
            if (isUnsorted) {
                unsortedFilesOpened++;
                unsortedReads += reads.size();
            }
            // distinct stretches of the file, however the reads were ordered or interleaved between splits
            reads.sort(Comparator.comparingLong((long[] read) -> read[1]));
            long end = Long.MIN_VALUE;
            for (long[] read : reads) {
                dataBytes += read[2];
                if (isUnsorted) {
                    unsortedBytesRead += read[2];
                }
                if (read[1] > end) {
                    ranges++;
                }
                end = Math.max(end, read[1] + read[2]);
            }
            // the definition used on the Spark side: a read that does not continue where the previous read of the file ended
            reads.sort(Comparator.comparingLong((long[] read) -> read[0]));
            end = Long.MIN_VALUE;
            for (long[] read : reads) {
                if (read[1] != end) {
                    rangesInReadOrder++;
                }
                end = read[1] + read[2];
            }
        }

        long splits = 0;
        for (OperatorStats operator : stats.getOperatorSummaries()) {
            if (operator.getOperatorType().contains("Scan")) {
                splits += operator.getTotalDrivers();
            }
        }

        // Every driver runs in a "split" span. The scan's drivers are those of the pipeline with the most of them,
        // or of the pipeline that read data files.
        Map<String, List<SpanData>> splitSpansByPipeline = new HashMap<>();
        String scanPipeline = null;
        for (SpanData span : spans) {
            if (span.getName().equals("split")) {
                String pipeline = span.getAttributes().get(PIPELINE_ID);
                splitSpansByPipeline.computeIfAbsent(pipeline, _ -> new ArrayList<>()).add(span);
                if (splitsWithReads.contains(span.getSpanId())) {
                    scanPipeline = pipeline;
                }
            }
        }
        if (scanPipeline == null) {
            scanPipeline = splitSpansByPipeline.entrySet().stream()
                    .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                    .map(Map.Entry::getKey)
                    .orElse(null);
        }
        long withReads = 0;
        long withoutReads = 0;
        double withReadsMillis = 0;
        double withReadsCpuMillis = 0;
        double withoutReadsMillis = 0;
        double withoutReadsCpuMillis = 0;
        for (SpanData span : splitSpansByPipeline.getOrDefault(scanPipeline, ImmutableList.of())) {
            double millis = (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1e6;
            Long cpuNanos = span.getAttributes().get(SPLIT_CPU_TIME_NANOS);
            double cpuMillis = cpuNanos == null ? 0 : cpuNanos / 1e6;
            if (splitsWithReads.contains(span.getSpanId())) {
                withReads++;
                withReadsMillis += millis;
                withReadsCpuMillis += cpuMillis;
            }
            else {
                withoutReads++;
                withoutReadsMillis += millis;
                withoutReadsCpuMillis += cpuMillis;
            }
        }

        Map<String, Long> indexMetrics = new HashMap<>();
        for (StageInfo stage : queryInfo.getStages().orElseThrow().getStages()) {
            for (Metrics metrics : stage.getStageStats().getSplitSourceMetrics().values()) {
                for (Map.Entry<String, Metric<?>> metric : metrics.getMetrics().entrySet()) {
                    if (metric.getKey().startsWith("fileIndex") && metric.getValue() instanceof LongCount count) {
                        indexMetrics.merge(metric.getKey(), count.getTotal(), Long::sum);
                    }
                }
            }
        }

        Set<List<Object>> rows = new HashSet<>();
        for (MaterializedRow row : result.result().getMaterializedRows()) {
            rows.add(row.getFields());
        }
        if (rows.size() != result.result().getRowCount()) {
            throw new IllegalStateException("Query returned duplicate rows");
        }
        return new Execution(
                new Measurement(
                        rows.size(),
                        wallMillis,
                        stats.getElapsedTime().toMillis(),
                        stats.getPlanningTime().toMillis(),
                        stats.getTotalCpuTime().toMillis(),
                        stats.getPlanningCpuTime().toMillis(),
                        processCpuMillis,
                        splitSourceMillis,
                        splitBatchMillis,
                        splits,
                        withReads,
                        withoutReads,
                        withoutReadsMillis,
                        withoutReadsCpuMillis,
                        withReadsMillis,
                        withReadsCpuMillis,
                        footerReads,
                        dataBytes,
                        ranges,
                        rangesInReadOrder,
                        readsByFile.size(),
                        dataReads,
                        stats.getPhysicalInputDataSize().toBytes(),
                        stats.getPhysicalInputPositions(),
                        unsorted.size(),
                        unsorted.values().stream().mapToLong(Long::longValue).sum(),
                        unsortedFilesOpened,
                        unsortedReads,
                        unsortedBytesRead,
                        manifestReads,
                        indexMetrics.containsKey("fileIndexFiles"),
                        indexMetrics.getOrDefault("fileIndexFiles", 0L),
                        indexMetrics.getOrDefault("fileIndexPartitionCandidates", 0L),
                        indexMetrics.getOrDefault("fileIndexColumnCandidates", 0L)),
                rows,
                threadCpuMillis);
    }

    private static Map<Long, Long> threadCpuNanos()
    {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        Map<Long, Long> cpuNanos = new HashMap<>();
        for (long id : threads.getAllThreadIds()) {
            cpuNanos.put(id, threads.getThreadCpuTime(id));
        }
        return cpuNanos;
    }

    /**
     * CPU time used since the given reading by the Java threads that are still alive, summed over threads whose
     * names are equal up to the first digit. The JIT compiler and garbage collector are not Java threads.
     */
    private static Map<String, Double> threadCpuMillis(Map<Long, Long> startCpuNanos)
    {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        Map<String, Double> cpuMillis = new HashMap<>();
        for (ThreadInfo thread : threads.getThreadInfo(threads.getAllThreadIds())) {
            if (thread == null) {
                continue;
            }
            long nanos = threads.getThreadCpuTime(thread.getThreadId());
            if (nanos < 0) {
                continue;
            }
            String name = thread.getThreadName().replaceFirst("[-_.]?[0-9].*", "").replace(',', ' ');
            cpuMillis.merge(name, (nanos - startCpuNanos.getOrDefault(thread.getThreadId(), 0L)) / 1e6, Double::sum);
        }
        return cpuMillis;
    }

    private void write(LookupCase lookupCase, Variant variant, String runs, String day, int keys, Measurement measurement, boolean matchesReference)
    {
        results.println(format(
                Locale.ENGLISH,
                "%s,%s,%s,%d,%s,%s,%s,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%s",
                lookupCase.name(),
                lookupCase.tag(),
                lookupCase.mix(),
                lookupCase.pairs(),
                variant.name(),
                runs,
                day,
                keys,
                measurement.rows(),
                measurement.wallMillis(),
                measurement.elapsedMillis(),
                measurement.planningMillis(),
                measurement.cpuMillis(),
                measurement.planningCpuMillis(),
                measurement.processCpuMillis(),
                measurement.splitSourceMillis(),
                measurement.splitBatchMillis(),
                measurement.splits(),
                measurement.splitsWithReads(),
                measurement.splitsWithoutReads(),
                measurement.splitsWithoutReadsMillis(),
                measurement.splitsWithoutReadsCpuMillis(),
                measurement.splitsWithReadsMillis(),
                measurement.splitsWithReadsCpuMillis(),
                measurement.footerReads(),
                measurement.dataBytes(),
                measurement.ranges(),
                measurement.rangesInReadOrder(),
                measurement.dataFilesOpened(),
                measurement.dataReads(),
                measurement.physicalInputBytes(),
                measurement.physicalInputRows(),
                measurement.unsortedFiles(),
                measurement.unsortedFileBytes(),
                measurement.unsortedFilesOpened(),
                measurement.unsortedReads(),
                measurement.unsortedBytesRead(),
                measurement.manifestReads(),
                measurement.indexUsed(),
                measurement.indexFiles(),
                measurement.indexPartitionCandidates(),
                measurement.indexColumnCandidates(),
                matchesReference));
    }

    private static Map<String, Long> fileSizes(Table table, long snapshotId)
            throws IOException
    {
        Map<String, Long> sizes = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                String location = task.file().location();
                sizes.put(location.substring(location.lastIndexOf('/') + 1), task.file().fileSizeInBytes());
            }
        }
        return sizes;
    }

    private static List<String[]> readPairs(Path file)
            throws IOException
    {
        List<String[]> pairs = new ArrayList<>();
        List<String> lines = Files.readAllLines(file);
        for (String line : lines.subList(1, lines.size())) {
            pairs.add(line.split(","));
        }
        return pairs;
    }

    private record LookupCase(String name, String tag, String mix, int pairs, int expectedRows, List<String[]> dayAndKey)
    {
        Map<String, List<String>> keysByDay()
        {
            Map<String, List<String>> keysByDay = new LinkedHashMap<>();
            for (String[] pair : dayAndKey) {
                keysByDay.computeIfAbsent(pair[0], _ -> new ArrayList<>()).add(pair[1]);
            }
            return keysByDay;
        }
    }

    private record Variant(String name, String catalog, String table, Session session) {}

    private record Execution(Measurement measurement, Set<List<Object>> rows, Map<String, Double> threadCpuMillis) {}

    private record Measurement(
            long rows,
            double wallMillis,
            double elapsedMillis,
            double planningMillis,
            double cpuMillis,
            double planningCpuMillis,
            double processCpuMillis,
            double splitSourceMillis,
            double splitBatchMillis,
            long splits,
            long splitsWithReads,
            long splitsWithoutReads,
            double splitsWithoutReadsMillis,
            double splitsWithoutReadsCpuMillis,
            double splitsWithReadsMillis,
            double splitsWithReadsCpuMillis,
            long footerReads,
            long dataBytes,
            long ranges,
            long rangesInReadOrder,
            long dataFilesOpened,
            long dataReads,
            long physicalInputBytes,
            long physicalInputRows,
            long unsortedFiles,
            long unsortedFileBytes,
            long unsortedFilesOpened,
            long unsortedReads,
            long unsortedBytesRead,
            long manifestReads,
            boolean indexUsed,
            long indexFiles,
            long indexPartitionCandidates,
            long indexColumnCandidates)
    {
        static Measurement median(List<Measurement> runs)
        {
            return new Measurement(
                    (long) median(runs, Measurement::rows),
                    median(runs, Measurement::wallMillis),
                    median(runs, Measurement::elapsedMillis),
                    median(runs, Measurement::planningMillis),
                    median(runs, Measurement::cpuMillis),
                    median(runs, Measurement::planningCpuMillis),
                    median(runs, Measurement::processCpuMillis),
                    median(runs, Measurement::splitSourceMillis),
                    median(runs, Measurement::splitBatchMillis),
                    (long) median(runs, Measurement::splits),
                    (long) median(runs, Measurement::splitsWithReads),
                    (long) median(runs, Measurement::splitsWithoutReads),
                    median(runs, Measurement::splitsWithoutReadsMillis),
                    median(runs, Measurement::splitsWithoutReadsCpuMillis),
                    median(runs, Measurement::splitsWithReadsMillis),
                    median(runs, Measurement::splitsWithReadsCpuMillis),
                    (long) median(runs, Measurement::footerReads),
                    (long) median(runs, Measurement::dataBytes),
                    (long) median(runs, Measurement::ranges),
                    (long) median(runs, Measurement::rangesInReadOrder),
                    (long) median(runs, Measurement::dataFilesOpened),
                    (long) median(runs, Measurement::dataReads),
                    (long) median(runs, Measurement::physicalInputBytes),
                    (long) median(runs, Measurement::physicalInputRows),
                    (long) median(runs, Measurement::unsortedFiles),
                    (long) median(runs, Measurement::unsortedFileBytes),
                    (long) median(runs, Measurement::unsortedFilesOpened),
                    (long) median(runs, Measurement::unsortedReads),
                    (long) median(runs, Measurement::unsortedBytesRead),
                    (long) median(runs, Measurement::manifestReads),
                    runs.stream().allMatch(Measurement::indexUsed),
                    (long) median(runs, Measurement::indexFiles),
                    (long) median(runs, Measurement::indexPartitionCandidates),
                    (long) median(runs, Measurement::indexColumnCandidates));
        }

        Measurement plus(Measurement other)
        {
            return new Measurement(
                    rows + other.rows,
                    wallMillis + other.wallMillis,
                    elapsedMillis + other.elapsedMillis,
                    planningMillis + other.planningMillis,
                    cpuMillis + other.cpuMillis,
                    planningCpuMillis + other.planningCpuMillis,
                    processCpuMillis + other.processCpuMillis,
                    splitSourceMillis + other.splitSourceMillis,
                    splitBatchMillis + other.splitBatchMillis,
                    splits + other.splits,
                    splitsWithReads + other.splitsWithReads,
                    splitsWithoutReads + other.splitsWithoutReads,
                    splitsWithoutReadsMillis + other.splitsWithoutReadsMillis,
                    splitsWithoutReadsCpuMillis + other.splitsWithoutReadsCpuMillis,
                    splitsWithReadsMillis + other.splitsWithReadsMillis,
                    splitsWithReadsCpuMillis + other.splitsWithReadsCpuMillis,
                    footerReads + other.footerReads,
                    dataBytes + other.dataBytes,
                    ranges + other.ranges,
                    rangesInReadOrder + other.rangesInReadOrder,
                    dataFilesOpened + other.dataFilesOpened,
                    dataReads + other.dataReads,
                    physicalInputBytes + other.physicalInputBytes,
                    physicalInputRows + other.physicalInputRows,
                    unsortedFiles,
                    unsortedFileBytes,
                    unsortedFilesOpened + other.unsortedFilesOpened,
                    unsortedReads + other.unsortedReads,
                    unsortedBytesRead + other.unsortedBytesRead,
                    manifestReads + other.manifestReads,
                    indexUsed && other.indexUsed,
                    indexFiles + other.indexFiles,
                    indexPartitionCandidates + other.indexPartitionCandidates,
                    indexColumnCandidates + other.indexColumnCandidates);
        }

        private static double median(List<Measurement> runs, ToDoubleFunction<Measurement> value)
        {
            double[] sorted = runs.stream().mapToDouble(value).sorted().toArray();
            return sorted[sorted.length / 2];
        }
    }
}
