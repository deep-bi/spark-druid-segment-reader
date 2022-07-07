package bi.deep

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType


case class DruidDataReaderFactory(filePath: String, schema: StructType, config: Config)
  extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] = DruidDataReader(filePath, schema, config)
}
