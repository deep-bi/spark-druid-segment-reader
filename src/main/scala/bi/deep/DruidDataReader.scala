package bi.deep

import org.apache.druid.segment.{DruidRowConverter, QueryableIndexIndexableAdapter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{StructField, StructType}

import java.io.File


case class DruidDataReader(filePath: String, schema: StructType, config: Config)
  extends InputPartitionReader[InternalRow] with DruidSegmentReader {

  private var current: Option[InternalRow] = None
  private val targetRowSize: Int = schema.size
  private val timestampIdx: Option[Int] = {
    if (config.druidTimestamp != "") Option(schema.fieldIndex(config.druidTimestamp))
    else None
  }

  private lazy val rowConverter: DruidRowConverter = withSegment(filePath, config, filePath) { file =>
    val qi = indexIO.loadIndex(new File(file.getAbsolutePath))
    val qiia = new QueryableIndexIndexableAdapter(qi)
    val segmentSchema = DruidSchemaReader.readSparkSchema(qi)
    val rowFieldsNames = segmentSchema.fields.map(_.name).toSet

    val filteredTargetFields: Array[(StructField, Int)] = schema.fields
      .zipWithIndex
      .filter(f => rowFieldsNames.contains(f._1.name))

    DruidRowConverter(qiia, schema, segmentSchema, targetRowSize, filteredTargetFields, timestampIdx)
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