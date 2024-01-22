package bi.deep

import org.apache.spark.sql.types.{StructField, StructType}

case class SparkSchema(
                        dimensions: Array[StructField],
                        metrics: Array[StructField]) {

  lazy val mergedSchema: StructType = StructType(dimensions ++ metrics)
  lazy val structTypeDimensions: StructType = StructType(dimensions)
  lazy val structTypeMetrics: StructType = StructType(metrics)

  def updateDimensions(newDimensions: Array[StructField]): SparkSchema = {
    SparkSchema(newDimensions, metrics)
  }
}