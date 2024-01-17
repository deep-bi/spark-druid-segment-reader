package bi.deep

import bi.deep.segments.{SegmentStorage, Segment}
import org.apache.druid.common.guava.GuavaUtils
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter


class DruidDataSourceReader(config: Config) extends DataSourceReader {

  @transient
  private lazy val mainPath: Path = new Path(config.inputPath, config.dataSource)

  @transient
  private implicit lazy val fs: FileSystem = config.factory.fileSystemFor(mainPath)

  @transient
  private lazy val storage = SegmentStorage(mainPath)

  @transient
  private lazy val schemaReader = DruidSchemaReader(config)

  @transient
  private lazy val filesPaths: Seq[Segment] = storage.findValidSegments(config.startDate, config.endDate)

  @transient
  private lazy val schema: StructType = readSchema()

  override def readSchema(): StructType = schemaReader.calculateSchema(filesPaths).mergeSchema

  private def fileToReaderFactory(file: Segment): InputPartition[InternalRow] = {
    DruidDataReaderFactory(file.path.toString, schema, config)
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    filesPaths.toList.map(fileToReaderFactory).asJava
  }
}