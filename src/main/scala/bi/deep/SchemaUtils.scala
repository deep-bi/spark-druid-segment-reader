package bi.deep

import org.apache.druid.segment.column.ColumnCapabilities.CoercionLogic
import org.apache.druid.segment.column.{ColumnCapabilitiesImpl, ValueType}
import org.apache.spark.sql.types._

import scala.collection.mutable


object SchemaUtils {

  lazy val coercionLogic: CoercionLogic = new CoercionLogic() {
    override def dictionaryEncoded(): Boolean = false

    override def dictionaryValuesSorted(): Boolean = false

    override def dictionaryValuesUnique(): Boolean = false

    override def multipleValues(): Boolean = false

    override def hasNulls: Boolean = false
  }

  private def merge(
                     columnsA: Array[(String, ColumnCapabilitiesImpl)],
                     columnsB: Array[(String, ColumnCapabilitiesImpl)]): Array[(String, ColumnCapabilitiesImpl)] = {
    val accumulator = mutable.LinkedHashMap[String, ColumnCapabilitiesImpl](columnsA: _*)

    columnsB.foldLeft(accumulator) { case (merged, (name, capabilities)) =>
      merged += name -> ColumnCapabilitiesImpl.merge(capabilities, merged.getOrElse(name, null), coercionLogic)
    }.toArray
  }

  def mergeSchemas(schemaA: DruidSchema, schemaB: DruidSchema): DruidSchema = {
    DruidSchema(merge(schemaA.getDimensions, schemaB.getDimensions), merge(schemaA.getMetrics, schemaB.getMetrics))
  }

  private def structFields(columns: Array[(String, ColumnCapabilitiesImpl)]): Array[StructField] = {
    columns.map { case (name, capabilities) =>
      val basicSparkType = capabilities.getType match {
        case ValueType.FLOAT => FloatType
        case ValueType.LONG => LongType
        case ValueType.STRING => StringType
        case ValueType.DOUBLE => DoubleType
        case _ => throw new RuntimeException("Unsupported druid type: " + capabilities.getType)
      }

      val sparkType = if (capabilities.hasMultipleValues.isTrue) ArrayType(basicSparkType) else basicSparkType
      StructField(name, sparkType)
    }
  }

  def druidToSpark(druidSchema: DruidSchema): SparkSchema = {
    SparkSchema(structFields(druidSchema.getDimensions), structFields(druidSchema.getMetrics))
  }
}