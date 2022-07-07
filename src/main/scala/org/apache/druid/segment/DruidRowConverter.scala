package org.apache.druid.segment

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import java.util

class DruidRowConverter(rowIt: QueryableIndexIndexableAdapter#RowIteratorImpl, segmentSchema: StructType,
                        targetRowSize: Int, filteredTargetFields: Array[(StructField, Int)], timestampIdx: Option[Int])
  extends Iterator[Row] with AutoCloseable {

  override def hasNext: Boolean = rowIt.moveToNext

  override def next(): Row = druidRowToSparkRow(rowIt.getPointer)

  override def close(): Unit = rowIt.close()

  private def druidRowToSparkRow(row: RowPointer): Row = {
    val targetRow = new Array[Any](targetRowSize)

    filteredTargetFields.foreach { case (targetField, targetFieldIdx) =>
      val rowFieldIndex = segmentSchema.fieldIndex(targetField.name)
      val value = row.getDimensionSelector(rowFieldIndex).getObject
      targetRow(targetFieldIdx) = targetField.dataType match {
        case _: ArrayType => value match {
          case null => null
          case arrValue: util.List[_] => arrValue.toArray
          case strValue: String => Array(strValue)
          case _ =>
            throw new ClassCastException(s"Invalid type: ${value.getClass.toString}")
        }
        case _ => value match {
          case arrValue: util.List[_] =>
            throw new RuntimeException(s"Single value expected, but found ${arrValue.size} values: ${arrValue.toString}")
          case strValue => strValue
        }
      }
    }
    if (timestampIdx.isDefined)
      targetRow(timestampIdx.get) = row.getTimestamp

    new GenericRow(targetRow)
  }
}


object DruidRowConverter {

  def apply(qiia: QueryableIndexIndexableAdapter, segmentSchema: StructType, targetRowSize: Int,
            filteredTargetFields: Array[(StructField, Int)], timestampIdx: Option[Int]): DruidRowConverter = {
    new DruidRowConverter(qiia.getRows, segmentSchema, targetRowSize, filteredTargetFields, timestampIdx)
  }
}