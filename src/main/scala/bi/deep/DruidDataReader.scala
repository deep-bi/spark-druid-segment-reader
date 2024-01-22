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
    val queryableIndex = indexIO.loadIndex(new File(file.getAbsolutePath))
    val queryableIndexIndexableAdapter = new QueryableIndexIndexableAdapter(queryableIndex)
    val queryableIndexStorageAdapter = new QueryableIndexStorageAdapter(queryableIndex)
    val druidSegmentSchema = DruidSchemaReader.readSparkSchema(queryableIndexStorageAdapter)
    val druidDimensions = druidSegmentSchema.structTypeDimensions.fieldNames
    val druidMetrics = druidSegmentSchema.structTypeMetrics.fieldNames

    val selectedDimensions: Array[(StructField, Int)] = schema.fields
      .zipWithIndex
      .filter { case (field, _) => druidDimensions.contains(field.name) }

    val selectedMetrics: Array[(StructField, Int)] = schema.fields
      .zipWithIndex
      .filter { case (field, _) => druidMetrics.contains(field.name) }

    DruidRowConverter(queryableIndexIndexableAdapter, schema, druidSegmentSchema, selectedDimensions, selectedMetrics, timestampIdx)
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