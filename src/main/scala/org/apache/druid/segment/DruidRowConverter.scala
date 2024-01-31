package org.apache.druid.segment

import bi.deep.SparkSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter

class DruidRowConverter(rowIt: QueryableIndexIndexableAdapter#RowIteratorImpl, fullSchema: StructType,
                        sparkSchema: SparkSchema, selectedDimensions: Array[(StructField, Int)],
                        selectedMetrics: Array[(StructField, Int)], timestampIdx: Option[Int])
  extends Iterator[InternalRow] with AutoCloseable {

  private val unsafeProjection = UnsafeProjection.create(fullSchema)

  override def hasNext: Boolean = rowIt.moveToNext

  override def next(): InternalRow = druidRowToSparkRow(rowIt.getPointer)

  override def close(): Unit = rowIt.close()

  private def druidDimensionsToSpark(
                                      row: RowPointer,
                                      dimensionsFields: Array[(StructField, Int)],
                                      internal: SpecificInternalRow): SpecificInternalRow = {

    dimensionsFields.foreach { case (targetField, targetFieldIdx) =>
      val rowFieldIndex = sparkSchema.structTypeDimensions.fieldIndex(targetField.name)
      val selector = row.getDimensionSelector(rowFieldIndex)
      val value = selector.getObject

      internal(targetFieldIdx) = targetField.dataType match {
        case _: ArrayType => value match {
          case null => null
          case arrValue: util.List[String] => ArrayData.toArrayData(arrValue.asScala.map(UTF8String.fromString).toArray)
          case strValue: String => ArrayData.toArrayData(Array(UTF8String.fromString(strValue)))
          case _ =>
            throw new ClassCastException(s"Invalid type: ${value.getClass.toString}")
        }
        case _ => value match {
          case arrValue: util.List[_] =>
            throw new RuntimeException(s"Single value expected, but found ${arrValue.size} values: ${arrValue.toString}")
          case strValue: String => UTF8String.fromString(strValue)
          case null => null
        }
      }
    }
    internal
  }

  private def druidMetricsToSpark(
                                   row: RowPointer,
                                   metricsFields: Array[(StructField, Int)],
                                   internal: SpecificInternalRow): SpecificInternalRow = {
    metricsFields.foreach { case (targetField, targetFieldIdx) =>
      val rowFieldIndex = sparkSchema.structTypeMetrics.fieldIndex(targetField.name)
      val selector = row.getMetricSelector(rowFieldIndex)
      val value = selector.getObject

      internal(targetFieldIdx) = targetField.dataType match {
        case _: ArrayType => value match {
          case null => null
          case intValue: Int => ArrayData.toArrayData(intValue)
          case floatValue: Float => ArrayData.toArrayData(floatValue)
          case longValue: Long => ArrayData.toArrayData(longValue)
          case doubleValue: Double => ArrayData.toArrayData(doubleValue)
          case shortValue: Short => ArrayData.toArrayData(shortValue)
          case _ =>
            throw new ClassCastException(s"Invalid type: ${value.getClass.toString}")
        }
        case _ => value match {
          case arrValue: util.List[_] =>
            throw new RuntimeException(s"Single value expected, but found ${arrValue.size} values: ${arrValue.toString}")
          case intValue: Int => intValue
          case floatValue: Float => floatValue
          case longValue: Long => longValue
          case doubleValue: Double => doubleValue
          case shortValue: Short => shortValue
          case null => null
          case _ =>
            throw new ClassCastException(s"Invalid type: ${value.getClass.toString}")
        }
      }
    }
    internal
  }

  private def druidRowToSparkRow(row: RowPointer): InternalRow = {
    val internal = new SpecificInternalRow(fullSchema)

    druidDimensionsToSpark(row, selectedDimensions, internal)
    druidMetricsToSpark(row, selectedMetrics, internal)

    if (timestampIdx.isDefined)
      internal(timestampIdx.get) = row.getTimestamp

    unsafeProjection(internal)
  }
}


object DruidRowConverter {

  def apply(queryableIndexIndexableAdapter: QueryableIndexIndexableAdapter, fullSchema: StructType,
            segmentSchema: SparkSchema, selectedDimensions: Array[(StructField, Int)],
            selectedMetrics: Array[(StructField, Int)], timestampIdx: Option[Int]): DruidRowConverter = {
    new DruidRowConverter(queryableIndexIndexableAdapter.getRows, fullSchema, segmentSchema, selectedDimensions, selectedMetrics, timestampIdx)
  }
}