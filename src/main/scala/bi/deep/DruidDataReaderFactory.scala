package bi.deep

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType


case class DruidDataReaderFactory(filePath: String, schema: StructType, config: Config)
  extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = DruidDataReader(filePath, schema, config)
}