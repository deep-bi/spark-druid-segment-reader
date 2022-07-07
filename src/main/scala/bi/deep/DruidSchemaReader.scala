package bi.deep

import org.apache.druid.segment.QueryableIndex
import org.apache.druid.segment.column.ColumnCapabilitiesImpl
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.immutable.ListMap


class DruidSchemaReader(path: String, config: Config) extends DruidSegmentReader {

  def calculateSchema(files: Array[LocatedFileStatus]): StructType = {
    val druidSchemas = files.map { file =>
      withSegment(file.getPath.toString, config, path) { segmentDir =>
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