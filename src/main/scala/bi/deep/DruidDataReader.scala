package bi.deep

import org.apache.druid.segment.{DruidRowConverter, QueryableIndexIndexableAdapter, QueryableIndexStorageAdapter}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{StructField, StructType}

import java.io.File


case class DruidDataReader(filePath: String, schema: StructType, config: Config)
  extends InputPartitionReader[InternalRow] with DruidSegmentReader {

  @transient
  private implicit val fs: FileSystem = config.factory.fileSystemFor(filePath)
  private var current: Option[InternalRow] = None
  private val timestampIdx: Option[Int] = {
    if (config.druidTimestamp != "") Option(schema.fieldIndex(config.druidTimestamp))
    else None
  }

  private lazy val rowConverter: DruidRowConverter = withSegment(filePath, config) { file =>
    val qi = indexIO.loadIndex(new File(file.getAbsolutePath))
    val qiia = new QueryableIndexIndexableAdapter(qi)
    val qisa = new QueryableIndexStorageAdapter(qi)
    val segmentSchema = DruidSchemaReader.readSparkSchema(qisa)
    val rowDimensionsNames = segmentSchema.structTypeDimensions.fieldNames
    val rowMetricsNames = segmentSchema.structTypeMetrics.fieldNames
//      .mergeSchema.fields.map(_.name).toSet

    val filteredTargetDimensions: Array[(StructField, Int)] = schema.fields
      .zipWithIndex
      .filter(f => rowDimensionsNames.contains(f._1.name))

    val filteredTargetMetrics: Array[(StructField, Int)] = schema.fields
      .zipWithIndex
      .filter(f => rowMetricsNames.contains(f._1.name))

    DruidRowConverter(qiia, schema, segmentSchema, filteredTargetDimensions, filteredTargetMetrics, timestampIdx)
  }

  override def next(): Boolean = {
    val hadNext = rowConverter.hasNext
    if (hadNext) current = Some(rowConverter.next())
    hadNext
  }

  override def get(): InternalRow = current.get

  override def close(): Unit = rowConverter.close()
}

object DruidDataReader {}