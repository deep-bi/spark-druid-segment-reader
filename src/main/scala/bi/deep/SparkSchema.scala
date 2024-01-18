package bi.deep

import org.apache.spark.sql.types.{StructField, StructType}

class SparkSchema(
                   var dimensions: Array[StructField],
                   var metrics: Array[StructField]) {

  lazy val mergedSchema: StructType = StructType(dimensions ++ metrics)
  lazy val structTypeDimensions: StructType = StructType(dimensions)
  lazy val structTypeMetrics: StructType = StructType(metrics)

  def updateDimensions(newDimensions: Array[StructField]): Unit = {
    dimensions = newDimensions
  }
}

object SparkSchema {
  def apply(
             dimensions: Array[StructField],
             metrics: Array[StructField]): SparkSchema = new SparkSchema(dimensions, metrics)
}