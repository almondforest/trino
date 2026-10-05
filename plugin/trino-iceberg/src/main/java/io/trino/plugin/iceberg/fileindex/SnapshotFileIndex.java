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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.ExpressionConverter.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.SnapshotUtil.schemaFor;

/**
 * All live data files of one table snapshot, held in memory together with the delete files that apply to them,
 * and organized so that the files matching a filter are found without reading manifests.
 * <p>
 * A lookup narrows the files in three steps. The first two may keep files that cannot match, but never drop
 * a file that can; the last one applies the same per-file test as {@link TableScan#planFiles()}.
 * <ol>
 * <li>Files are grouped by partition, and the filter is evaluated once per distinct partition.</li>
 * <li>For each indexed column the filter constrains, an {@link IntervalIndex} over the files' value bounds
 * finds the files whose bounds overlap the constraint.</li>
 * <li>The remaining files are checked against their column statistics.</li>
 * </ol>
 * A snapshot never changes, so an index never becomes stale.
 */
public final class SnapshotFileIndex
{
    private final Schema schema;
    private final String schemaJson;
    private final Map<Integer, String> specJsons;
    private final DataFile[] files;
    private final DeleteFile[][] deleteFiles;
    private final List<PartitionGroup> partitionGroups;
    private final Set<Integer> statisticsColumnIds;
    private final Map<Integer, ColumnIndex<?>> columnIndexes;

    private SnapshotFileIndex(
            Schema schema,
            Map<Integer, PartitionSpec> specs,
            List<DataFile> files,
            List<DeleteFile[]> deleteFiles,
            List<NestedField> statisticsColumns,
            TypeManager typeManager)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.schemaJson = SchemaParser.toJson(schema);
        this.files = files.toArray(DataFile[]::new);
        this.deleteFiles = deleteFiles.toArray(DeleteFile[][]::new);
        this.statisticsColumnIds = statisticsColumns.stream()
                .map(NestedField::fieldId)
                .collect(toImmutableSet());

        ImmutableMap.Builder<Integer, String> specJsons = ImmutableMap.builder();
        specs.forEach((specId, spec) -> specJsons.put(specId, PartitionSpecParser.toJson(spec)));
        this.specJsons = specJsons.buildOrThrow();

        this.partitionGroups = groupByPartition(specs, this.files);

        ImmutableMap.Builder<Integer, ColumnIndex<?>> columnIndexes = ImmutableMap.builder();
        for (NestedField column : statisticsColumns) {
            if (isIntervalIndexSupported(column.type())) {
                columnIndexes.put(column.fieldId(), ColumnIndex.create(column, toTrinoType(column.type(), typeManager), this.files));
            }
        }
        this.columnIndexes = columnIndexes.buildOrThrow();
    }

    /**
     * Reads the file list of the snapshot. Returns empty if the snapshot has more than {@code maxFiles} data files.
     *
     * @param columnNames top-level columns to keep statistics for and to index; names that do not resolve to a
     *         primitive top-level column are ignored
     */
    public static Optional<SnapshotFileIndex> build(
            Table table,
            long snapshotId,
            List<String> columnNames,
            TypeManager typeManager,
            ExecutorService planningExecutor,
            long maxFiles)
    {
        checkArgument(maxFiles > 0, "maxFiles must be positive");
        Schema schema = schemaFor(table, snapshotId);
        Map<String, NestedField> statisticsColumns = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            NestedField column = schema.caseInsensitiveFindField(columnName);
            if (column != null && column.type().isPrimitiveType() && schema.asStruct().field(column.fieldId()) != null) {
                statisticsColumns.put(column.name(), column);
            }
        }

        TableScan scan = table.newScan()
                .useSnapshot(snapshotId)
                .planWith(planningExecutor);
        if (!statisticsColumns.isEmpty()) {
            scan = scan.includeColumnStats(statisticsColumns.keySet());
        }

        List<DataFile> files = new ArrayList<>();
        List<DeleteFile[]> deleteFiles = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                if (files.size() >= maxFiles) {
                    return Optional.empty();
                }
                files.add(task.file());
                deleteFiles.add(task.deletes().toArray(DeleteFile[]::new));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return Optional.of(new SnapshotFileIndex(
                schema,
                table.specs(),
                files,
                deleteFiles,
                ImmutableList.copyOf(statisticsColumns.values()),
                typeManager));
    }

    public int fileCount()
    {
        return files.length;
    }

    /**
     * Whether file statistics are held for all the given columns. A lookup only prunes as well as
     * {@link TableScan#planFiles()} when this holds for every column the filter refers to.
     */
    public boolean hasStatisticsFor(Set<Integer> columnIds)
    {
        return statisticsColumnIds.containsAll(columnIds);
    }

    /**
     * Finds the files that may contain rows matching the filter.
     *
     * @param filter the filter as an Iceberg expression
     * @param predicate the same filter as a tuple domain
     */
    public FileIndexScan planFiles(Expression filter, TupleDomain<IcebergColumnHandle> predicate)
    {
        requireNonNull(filter, "filter is null");
        requireNonNull(predicate, "predicate is null");

        if (filter.op() == Expression.Operation.FALSE || predicate.isNone()) {
            return new FileIndexScan(CloseableIterable.empty(), files.length, 0, 0, new AtomicLong());
        }

        BitSet candidates = partitionCandidates(filter);
        int partitionCandidates = candidates.cardinality();

        for (Map.Entry<IcebergColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
            ColumnIndex<?> columnIndex = columnIndexes.get(entry.getKey().getId());
            if (columnIndex != null) {
                columnIndex.overlapping(entry.getValue(), files.length).ifPresent(candidates::and);
            }
        }
        int columnCandidates = candidates.cardinality();

        InclusiveMetricsEvaluator metricsEvaluator = new InclusiveMetricsEvaluator(schema, filter);
        ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);
        AtomicLong matchedFiles = new AtomicLong();
        Iterable<FileScanTask> tasks = () -> new AbstractIterator<>()
        {
            private int next = candidates.nextSetBit(0);

            @Override
            protected FileScanTask computeNext()
            {
                while (next >= 0) {
                    int file = next;
                    next = candidates.nextSetBit(file + 1);
                    if (metricsEvaluator.eval(files[file])) {
                        matchedFiles.incrementAndGet();
                        return new BaseFileScanTask(files[file], deleteFiles[file], schemaJson, specJsons.get(files[file].specId()), residuals);
                    }
                }
                return endOfData();
            }
        };
        return new FileIndexScan(CloseableIterable.withNoopClose(tasks), files.length, partitionCandidates, columnCandidates, matchedFiles);
    }

    private BitSet partitionCandidates(Expression filter)
    {
        BitSet candidates = new BitSet(files.length);
        for (PartitionGroup group : partitionGroups) {
            if (group.spec().isUnpartitioned() || filter.op() == Expression.Operation.TRUE) {
                group.addAll(candidates);
                continue;
            }
            Evaluator evaluator = new Evaluator(group.spec().partitionType(), Projections.inclusive(group.spec()).project(filter));
            for (int i = 0; i < group.partitions().length; i++) {
                if (evaluator.eval(group.partitions()[i])) {
                    for (int file : group.files()[i]) {
                        candidates.set(file);
                    }
                }
            }
        }
        return candidates;
    }

    private static List<PartitionGroup> groupByPartition(Map<Integer, PartitionSpec> specs, DataFile[] files)
    {
        Map<Integer, Map<StructLikeWrapper, List<Integer>>> filesBySpecAndPartition = new LinkedHashMap<>();
        Map<Integer, StructLikeWrapper> wrappers = new LinkedHashMap<>();
        for (int file = 0; file < files.length; file++) {
            int specId = files[file].specId();
            PartitionSpec spec = requireNonNull(specs.get(specId), () -> "Table specs do not contain spec " + specId);
            StructLikeWrapper wrapper = wrappers.computeIfAbsent(specId, _ -> StructLikeWrapper.forType(spec.partitionType()));
            filesBySpecAndPartition.computeIfAbsent(specId, _ -> new LinkedHashMap<>())
                    .computeIfAbsent(wrapper.copyFor(files[file].partition()), _ -> new ArrayList<>())
                    .add(file);
        }

        ImmutableList.Builder<PartitionGroup> groups = ImmutableList.builder();
        filesBySpecAndPartition.forEach((specId, filesByPartition) -> {
            StructLike[] partitions = new StructLike[filesByPartition.size()];
            int[][] partitionFiles = new int[filesByPartition.size()][];
            int position = 0;
            for (Map.Entry<StructLikeWrapper, List<Integer>> entry : filesByPartition.entrySet()) {
                partitions[position] = entry.getKey().get();
                partitionFiles[position] = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
                position++;
            }
            groups.add(new PartitionGroup(specs.get(specId), partitions, partitionFiles));
        });
        return groups.build();
    }

    /**
     * Limited to types for which the value Trino pushes down and the value stored in file statistics compare
     * the same way in Trino and in Iceberg. Floating point types are left out because of NaN.
     */
    private static boolean isIntervalIndexSupported(org.apache.iceberg.types.Type type)
    {
        return switch (type.typeId()) {
            case INTEGER, LONG, DATE, TIME, TIMESTAMP, STRING, DECIMAL -> true;
            default -> false;
        };
    }

    public record FileIndexScan(CloseableIterable<FileScanTask> tasks, int indexedFiles, int partitionCandidates, int columnCandidates, AtomicLong matchedFiles)
    {
        public FileIndexScan
        {
            requireNonNull(tasks, "tasks is null");
            requireNonNull(matchedFiles, "matchedFiles is null");
        }
    }

    private record PartitionGroup(PartitionSpec spec, StructLike[] partitions, int[][] files)
    {
        void addAll(BitSet candidates)
        {
            for (int[] partitionFiles : files) {
                for (int file : partitionFiles) {
                    candidates.set(file);
                }
            }
        }
    }

    /**
     * @param <T> type of the query bounds the interval index takes
     * @param toIndexBound converts a bound of a Trino range to the type the interval index takes
     */
    private record ColumnIndex<T>(Type type, IntervalIndex<T> intervals, Function<Object, T> toIndexBound, int[] filesWithoutBounds)
    {
        static ColumnIndex<?> create(NestedField column, Type type, DataFile[] files)
        {
            int fieldId = column.fieldId();
            List<Integer> boundedFiles = new ArrayList<>();
            List<ByteBuffer> lowers = new ArrayList<>();
            List<ByteBuffer> uppers = new ArrayList<>();
            List<Integer> filesWithoutBounds = new ArrayList<>();
            for (int file = 0; file < files.length; file++) {
                ByteBuffer lower = bound(files[file].lowerBounds(), fieldId);
                ByteBuffer upper = bound(files[file].upperBounds(), fieldId);
                if (lower == null || upper == null) {
                    filesWithoutBounds.add(file);
                    continue;
                }
                boundedFiles.add(file);
                lowers.add(lower);
                uppers.add(upper);
            }
            int[] boundedFileIds = boundedFiles.stream().mapToInt(Integer::intValue).toArray();
            int[] fileIdsWithoutBounds = filesWithoutBounds.stream().mapToInt(Integer::intValue).toArray();

            if (column.type().typeId() == TypeID.STRING) {
                // Trino and Iceberg both order strings by code point, which is the order of their UTF-8 bytes.
                // The bounds stay encoded, and are compared with the bytes of the Trino value.
                return new ColumnIndex<>(type, IntervalIndex.forUtf8(boundedFileIds, lowers, uppers), Slice.class::cast, fileIdsWithoutBounds);
            }

            Comparator<Object> comparator = Comparators.forType(column.type().asPrimitiveType());
            return new ColumnIndex<>(
                    type,
                    IntervalIndex.forValues(comparator, boundedFileIds, decode(column, lowers), decode(column, uppers)),
                    value -> convertTrinoValueToIceberg(type, value),
                    fileIdsWithoutBounds);
        }

        /**
         * Files whose bounds overlap the domain, or empty if the domain cannot be used to narrow the files.
         */
        Optional<BitSet> overlapping(Domain domain, int fileCount)
        {
            // A file with NULLs matches a domain that allows NULL whatever its bounds are
            if (domain.isNullAllowed() || domain.getValues().isAll() || !domain.getType().equals(type)) {
                return Optional.empty();
            }
            BitSet overlapping = new BitSet(fileCount);
            for (int file : filesWithoutBounds) {
                overlapping.set(file);
            }
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                T low = null;
                if (!range.isLowUnbounded()) {
                    low = toIndexBound.apply(range.getLowBoundedValue());
                }
                T high = null;
                if (!range.isHighUnbounded()) {
                    high = toIndexBound.apply(range.getHighBoundedValue());
                }
                intervals.collectOverlapping(low, range.isLowInclusive(), high, range.isHighInclusive(), overlapping);
            }
            return Optional.of(overlapping);
        }

        private static List<Object> decode(NestedField column, List<ByteBuffer> bounds)
        {
            List<Object> values = new ArrayList<>(bounds.size());
            for (ByteBuffer bound : bounds) {
                values.add(Conversions.fromByteBuffer(column.type(), bound));
            }
            return values;
        }

        private static ByteBuffer bound(Map<Integer, ByteBuffer> bounds, int fieldId)
        {
            if (bounds == null) {
                return null;
            }
            return bounds.get(fieldId);
        }
    }
}
