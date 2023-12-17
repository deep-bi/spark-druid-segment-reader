package bi.deep

import bi.deep.segments.Segment
import org.apache.druid.segment.column.{ColumnCapabilities, ColumnCapabilitiesImpl}
import org.apache.druid.segment.{QueryableIndex, StorageAdapter, QueryableIndexStorageAdapter}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.JavaConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter, iterableAsScalaIterableConverter}
import scala.collection.immutable
import scala.collection.immutable.ListMap


case class DruidSchemaReader(config: Config) extends DruidSegmentReader {
  def calculateSchema(files: Seq[Segment])(implicit fs: FileSystem): StructType = {
    val druidSchemas = files.map { file =>
      withSegment(file.path.toString, config) { segmentDir =>
        val qi = indexIO.loadIndex(segmentDir)
        val qisa = new QueryableIndexStorageAdapter(qi)
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
  def readSparkSchema(qisa: QueryableIndexStorageAdapter): StructType = SchemaUtils.druidToSpark(readDruidSchema(qisa))

  def readDruidSchema(qi: QueryableIndex): ListMap[String, ColumnCapabilitiesImpl] = {
    val handlers = qi.getDimensionHandlers.values.asScala.toArray
    val capabilities = handlers.map { handler =>
      (handler.getDimensionName, qi.getColumnHolder(handler.getDimensionName).getCapabilities.asInstanceOf[ColumnCapabilitiesImpl])
    }
    ListMap(capabilities: _*)
  }

  def readDruidSchema(qisa: QueryableIndexStorageAdapter): ListMap[String, ColumnCapabilitiesImpl] = {
    val metrics = qisa.getAvailableMetrics.asScala.toArray
    val dimensions = qisa.getAvailableDimensions.iterator().asScala.toArray

    val capabilities = metrics.map { metric =>
      (metric, qisa.getColumnCapabilities(metric).asInstanceOf[ColumnCapabilitiesImpl])
    }

    val dCapabilities = dimensions.map { dimension =>
      (dimension, qisa.getColumnCapabilities(dimension).asInstanceOf[ColumnCapabilitiesImpl])
    }

    ListMap(immutable.Seq.concat(capabilities, dCapabilities): _*)
  }
}