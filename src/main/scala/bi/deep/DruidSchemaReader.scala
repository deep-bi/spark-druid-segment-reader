package bi.deep

import bi.deep.segments.Segment
import org.apache.druid.segment.column.ColumnCapabilitiesImpl
import org.apache.druid.segment.{QueryableIndex, QueryableIndexStorageAdapter}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.JavaConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter, iterableAsScalaIterableConverter}
import scala.collection.immutable
import scala.collection.immutable.ListMap


case class DruidSchemaReader(config: Config) extends DruidSegmentReader {
  def calculateSchema(files: Seq[Segment])(implicit fs: FileSystem): SparkSchema = {
    val druidSchemas = files.map { file =>
      withSegment(file.path.toString, config) { segmentDir =>
        val qi = indexIO.loadIndex(segmentDir)
        val qisa = new QueryableIndexStorageAdapter(qi)
        DruidSchemaReader.readDruidSchema(qisa)
      }
    }

    druidSchemas.foreach(druidSchema => druidSchema.validateFields())

    val sparkSchema = SchemaUtils.druidToSpark(druidSchemas.reduce((s1, s2) => SchemaUtils.mergeSchemas(s1, s2)))
    val sparkDimensions = sparkSchema.structTypeDimensions

    if (config.druidTimestamp != "") {
      if (sparkDimensions.fieldNames.contains(config.druidTimestamp)) {
        sparkDimensions.fields(sparkDimensions.fieldIndex(config.druidTimestamp)) = StructField(config.druidTimestamp, LongType)
        sparkSchema.updateDimensions(sparkDimensions.fields)
        sparkSchema
      }
      else {
        sparkSchema.updateDimensions(sparkDimensions.add(config.druidTimestamp, LongType).fields)
        sparkSchema
      }
    }
    else {
      sparkSchema
    }
  }
}

object DruidSchemaReader {
  def readSparkSchema(qisa: QueryableIndexStorageAdapter): SparkSchema = SchemaUtils.druidToSpark(readDruidSchema(qisa))

  private def readDruidSchema(qisa: QueryableIndexStorageAdapter): DruidSchema = {

    val metrics = qisa.getAvailableMetrics.asScala.toArray
    val dimensions = qisa.getAvailableDimensions.iterator().asScala.toArray

    val capabilities = metrics.map { metric =>
      (metric, qisa.getColumnCapabilities(metric).asInstanceOf[ColumnCapabilitiesImpl])
    }

    val dCapabilities = dimensions.map { dimension =>
      (dimension, qisa.getColumnCapabilities(dimension).asInstanceOf[ColumnCapabilitiesImpl])
    }

    DruidSchema(dCapabilities, capabilities)
  }
}