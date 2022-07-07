package bi.deep

import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}


class DruidSource extends DataSourceV2 with ReadSupport with DataSourceRegister {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new DruidDataSourceReader(Config(options, SparkContext.getOrCreate().hadoopConfiguration))
  }

  override def shortName(): String = "druid-segment"
}