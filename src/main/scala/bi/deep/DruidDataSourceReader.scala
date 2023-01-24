package bi.deep

import org.apache.druid.common.guava.GuavaUtils
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter


class DruidDataSourceReader(config: Config) extends DataSourceReader {

  val mainPath: String = config.inputPath + config.dataSource + "/"
  private lazy val filesPaths: Array[LocatedFileStatus] = LatestSegmentSelector(config, mainPath).getPathsArray
  private lazy val schema: StructType = readSchema()



  override def readSchema(): StructType = {
    val schemaReader = new DruidSchemaReader(mainPath, config)
    schemaReader.calculateSchema(filesPaths)
  }

  private def fileToReaderFactory(file: LocatedFileStatus): InputPartition[InternalRow] = {
    DruidDataReaderFactory(file.getPath.toString, schema, config)
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    filesPaths.toList.map(fileToReaderFactory).asJava
  }
}