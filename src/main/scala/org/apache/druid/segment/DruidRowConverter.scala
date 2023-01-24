package org.apache.druid.segment

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter

class DruidRowConverter(rowIt: QueryableIndexIndexableAdapter#RowIteratorImpl, fullSchema: StructType, segmentSchema: StructType,
                        targetRowSize: Int, filteredTargetFields: Array[(StructField, Int)], timestampIdx: Option[Int])
  extends Iterator[InternalRow] with AutoCloseable {

  private val unsafeProjection = UnsafeProjection.create(fullSchema)

  override def hasNext: Boolean = rowIt.moveToNext

  override def next(): InternalRow = druidRowToSparkRow(rowIt.getPointer)

  override def close(): Unit = rowIt.close()

  private def druidRowToSparkRow(row: RowPointer): InternalRow = {
    val internal = new SpecificInternalRow(fullSchema)

    filteredTargetFields.foreach { case (targetField, targetFieldIdx) =>
      val rowFieldIndex = segmentSchema.fieldIndex(targetField.name)
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
    if (timestampIdx.isDefined)
      internal(timestampIdx.get) = row.getTimestamp

    unsafeProjection(internal)
    //    internal
  }
}


object DruidRowConverter {

  def apply(qiia: QueryableIndexIndexableAdapter, fullSchema: StructType, segmentSchema: StructType, targetRowSize: Int,
            filteredTargetFields: Array[(StructField, Int)], timestampIdx: Option[Int]): DruidRowConverter = {
    new DruidRowConverter(qiia.getRows, fullSchema, segmentSchema, targetRowSize, filteredTargetFields, timestampIdx)
  }
}