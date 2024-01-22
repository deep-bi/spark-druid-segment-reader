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
  def calculateSparkSchema(files: Seq[Segment])(implicit fs: FileSystem): SparkSchema = {
    var druidSchemas = files.map { file =>
      withSegment(file.path.toString, config) { segmentDir =>
        val queryableIndex = indexIO.loadIndex(segmentDir)
        val queryableIndexStorageAdapter = new QueryableIndexStorageAdapter(queryableIndex)
        DruidSchemaReader.readDruidSchema(queryableIndexStorageAdapter)
      }
    }

    druidSchemas = druidSchemas.map(druidSchema => druidSchema.validateFields())

    val sparkSchema = SchemaUtils.druidToSpark(druidSchemas.reduce(SchemaUtils.mergeSchemas))
    val sparkDimensions = sparkSchema.structTypeDimensions

    if (config.druidTimestamp.isEmpty) sparkSchema
    else {
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
  }
}

object DruidSchemaReader {
  def readSparkSchema(queryableIndexStorageAdapter: QueryableIndexStorageAdapter):
  SparkSchema = SchemaUtils.druidToSpark(readDruidSchema(queryableIndexStorageAdapter))

  private def readDruidSchema(queryableIndexStorageAdapter: QueryableIndexStorageAdapter): DruidSchema = {

    val metrics = queryableIndexStorageAdapter.getAvailableMetrics.asScala.toArray
    val dimensions = queryableIndexStorageAdapter.getAvailableDimensions.iterator().asScala.toArray

    val dCapabilities = dimensions.map { dimension =>
      (dimension, queryableIndexStorageAdapter.getColumnCapabilities(dimension).asInstanceOf[ColumnCapabilitiesImpl])
    }

    val mCapabilities = metrics.map { metric =>
      (metric, queryableIndexStorageAdapter.getColumnCapabilities(metric).asInstanceOf[ColumnCapabilitiesImpl])
    }

    DruidSchema(dCapabilities, mCapabilities)
  }
}