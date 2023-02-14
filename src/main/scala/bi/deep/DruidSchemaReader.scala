package bi.deep

import bi.deep.segments.Segment
import org.apache.druid.segment.QueryableIndex
import org.apache.druid.segment.column.ColumnCapabilitiesImpl
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.immutable.ListMap


case class DruidSchemaReader(config: Config) extends DruidSegmentReader {
  def calculateSchema(files: Seq[Segment])(implicit fs: FileSystem): StructType = {
    val druidSchemas = files.map { file =>
      withSegment(file.path.toString, config) { segmentDir =>
        val qi = indexIO.loadIndex(segmentDir)
        DruidSchemaReader.readDruidSchema(qi)
      }
    }.map(SchemaUtils.validateFields)

    val merged = SchemaUtils.druidToSpark(druidSchemas.reduce((s1, s2) => SchemaUtils.merge(s1, s2)))

    if (config.druidTimestamp != "")
      if (merged.fieldNames.contains(config.druidTimestamp)) {
        merged.fields(merged.fieldIndex(config.druidTimestamp)) = StructField(config.druidTimestamp, LongType)
        merged
      }
      else
        merged.add(config.druidTimestamp, LongType)
    else
      merged
  }
}

object DruidSchemaReader {
  def readSparkSchema(qi: QueryableIndex): StructType = SchemaUtils.druidToSpark(readDruidSchema(qi))

  def readDruidSchema(qi: QueryableIndex): ListMap[String, ColumnCapabilitiesImpl] = {
    val handlers = qi.getDimensionHandlers.values.asScala.toArray
    val capabilities = handlers.map { handler =>
      (handler.getDimensionName, qi.getColumnHolder(handler.getDimensionName).getCapabilities.asInstanceOf[ColumnCapabilitiesImpl])
    }
    ListMap(capabilities: _*)
  }
}