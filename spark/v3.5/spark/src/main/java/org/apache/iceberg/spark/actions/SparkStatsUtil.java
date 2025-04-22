/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.stats.ThetaSketchAgg;

public class SparkStatsUtil {

  private SparkStatsUtil() {}

  public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";
  public static final String APACHE_DATASKETCHES_THETA_V1_MIN_PROPERTY = "min";
  public static final String APACHE_DATASKETCHES_THETA_V1_MAX_PROPERTY = "max";
  public static final String APACHE_DATASKETCHES_THETA_V1_NULL_COUNT_PROPERTY = "nullCount";

  static List<Blob> generateBlobs(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columns) {
    Row sketches = computeNDVSketches(spark, table, snapshot, columns);
    List<Blob> blobs = Lists.newArrayList();
    try (CloseableIterable<ScanTask> scanTasks =
        table.newBatchScan().useSnapshot(snapshot.snapshotId()).includeColumnStats().planFiles()) {
      StructLike minStats = computeMins(table, scanTasks);
      StructLike maxStats = computeMaxs(table, scanTasks);
      StructLike nullCounts = computeNullCounts(table, scanTasks);
      Schema schema = table.schemas().get(snapshot.schemaId());
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField field = schema.findField(columns.get(i));
        Sketch sketch = CompactSketch.wrap(Memory.wrap((byte[]) sketches.get(i)));
        blobs.add(
            toBlob(
                field,
                sketch,
                snapshot,
                StandardCharsets.UTF_8
                    .decode(
                        Conversions.toByteBuffer(
                            field.type(), minStats.get(i, field.type().typeId().javaClass())))
                    .toString(),
                StandardCharsets.UTF_8
                    .decode(
                        Conversions.toByteBuffer(
                            field.type(), maxStats.get(i, field.type().typeId().javaClass())))
                    .toString(),
                nullCounts.get(i, Long.class)));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return blobs;
  }

  private static Blob toBlob(
      Types.NestedField field,
      Sketch sketch,
      Snapshot snapshot,
      String min,
      String max,
      Long nullCount) {
    return new Blob(
        StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
        ImmutableList.of(field.fieldId()),
        snapshot.snapshotId(),
        snapshot.sequenceNumber(),
        ByteBuffer.wrap(sketch.toByteArray()),
        PuffinCompressionCodec.ZSTD,
        ImmutableMap.of(
            APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY,
            String.valueOf((long) sketch.getEstimate()),
            APACHE_DATASKETCHES_THETA_V1_MIN_PROPERTY,
            min,
            APACHE_DATASKETCHES_THETA_V1_MAX_PROPERTY,
            max,
            APACHE_DATASKETCHES_THETA_V1_NULL_COUNT_PROPERTY,
            String.valueOf(nullCount)));
  }

  private static Row computeNDVSketches(
      SparkSession spark, Table table, Snapshot snapshot, List<String> colNames) {
    Dataset<Row> inputDF = SparkTableUtil.loadTable(spark, table, snapshot.snapshotId());
    return inputDF.select(toAggColumns(colNames)).first();
  }

  private static StructLike computeMins(Table table, CloseableIterable<ScanTask> scanTasks) {
    List<Types.NestedField> fields = table.schema().columns();
    List<BoundAggregate<?, ?>> expressions = Lists.newArrayListWithExpectedSize(fields.size());
    fields.forEach(
        field -> {
          Expression expr = Expressions.min(field.name());
          Expression bound = Binder.bind(table.schema().asStruct(), expr, true);
          expressions.add((BoundAggregate<?, ?>) bound);
        });
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(expressions);
    for (ScanTask scanTask : scanTasks) {
      aggregateEvaluator.update(scanTask.asFileScanTask().file());
    }
    return aggregateEvaluator.result();
  }

  private static StructLike computeMaxs(Table table, CloseableIterable<ScanTask> scanTasks) {
    List<Types.NestedField> fields = table.schema().columns();
    List<BoundAggregate<?, ?>> expressions = Lists.newArrayListWithExpectedSize(fields.size());
    fields.forEach(
        field -> {
          Expression expr = Expressions.max(field.name());
          Expression bound = Binder.bind(table.schema().asStruct(), expr, true);
          expressions.add((BoundAggregate<?, ?>) bound);
        });
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(expressions);
    for (ScanTask scanTask : scanTasks) {
      aggregateEvaluator.update(scanTask.asFileScanTask().file());
    }
    return aggregateEvaluator.result();
  }

  private static StructLike computeNullCounts(Table table, CloseableIterable<ScanTask> scanTasks) {
    List<Types.NestedField> fields = table.schema().columns();
    List<BoundAggregate<?, ?>> expressions = Lists.newArrayListWithExpectedSize(fields.size());
    fields.forEach(
        field -> {
          Expression expr = Expressions.countNull(field.name());
          Expression bound = Binder.bind(table.schema().asStruct(), expr, true);
          expressions.add((BoundAggregate<?, ?>) bound);
        });
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(expressions);
    for (ScanTask scanTask : scanTasks) {
      aggregateEvaluator.update(scanTask.asFileScanTask().file());
    }
    return aggregateEvaluator.result();
  }

  private static Column[] toAggColumns(List<String> colNames) {
    return colNames.stream().map(SparkStatsUtil::toAggColumn).toArray(Column[]::new);
  }

  private static Column toAggColumn(String colName) {
    ThetaSketchAgg agg = new ThetaSketchAgg(colName);
    return new Column(agg.toAggregateExpression());
  }
}
