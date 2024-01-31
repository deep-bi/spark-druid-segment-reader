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
    val druidSchemas = files.map { file =>
      withSegment(file.path.toString, config) { segmentDir =>
        val queryableIndex = indexIO.loadIndex(segmentDir)
        val queryableIndexStorageAdapter = new QueryableIndexStorageAdapter(queryableIndex)
        DruidSchemaReader.readDruidSchema(queryableIndexStorageAdapter)
      }
    }

    val sparkSchema = SchemaUtils.druidToSpark(druidSchemas.reduce(SchemaUtils.mergeSchemas))
    val sparkDimensions = sparkSchema.structTypeDimensions

    if (config.druidTimestamp.isEmpty) sparkSchema
    else {
      if (sparkDimensions.fieldNames.contains(config.druidTimestamp)) {
        sparkDimensions.fields(sparkDimensions.fieldIndex(config.druidTimestamp)) = StructField(config.druidTimestamp, LongType)
        sparkSchema.copy(dimensions = sparkDimensions.fields)
      }
      else {
        sparkSchema.copy(dimensions = sparkDimensions.add(config.druidTimestamp, LongType).fields)
      }
    }
  }
}

object DruidSchemaReader {
  def readSparkSchema(queryableIndexStorageAdapter: QueryableIndexStorageAdapter):
  SparkSchema = SchemaUtils.druidToSpark(readDruidSchema(queryableIndexStorageAdapter))

  private def readDruidSchema(druidStorage: QueryableIndexStorageAdapter): DruidSchema = {

    val getCapabilities: String => (String, ColumnCapabilitiesImpl) = column => (column -> druidStorage.getColumnCapabilities(column).asInstanceOf[ColumnCapabilitiesImpl])

    val metrics = druidStorage.getAvailableMetrics.asScala
      .map(getCapabilities).toArray

    val dimensions = druidStorage.getAvailableDimensions.iterator().asScala
      .map(getCapabilities).toArray
    DruidSchema(dimensions, metrics)

  }
}