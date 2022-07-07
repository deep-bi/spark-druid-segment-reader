package bi.deep

import org.apache.druid.segment.column.ColumnCapabilities.CoercionLogic
import org.apache.druid.segment.column.{ColumnCapabilitiesImpl, ValueType}
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap
import scala.collection.mutable


object SchemaUtils {


  lazy val coercionLogic: CoercionLogic = new CoercionLogic() {
    override def dictionaryEncoded(): Boolean = false

    override def dictionaryValuesSorted(): Boolean = false

    override def dictionaryValuesUnique(): Boolean = false

    override def multipleValues(): Boolean = false

    override def hasNulls: Boolean = false
  }

  /** Validates the column names in the provided schema and removes all invalid characters, such as:
   * `,`, `;`, `{`, `}`, `(`, `)`, `=`, and all whitespace characters.
   *
   * @param schema Druid schema to validate
   * @return validated schema
   */
  def validateFields(schema: ListMap[String, ColumnCapabilitiesImpl]): ListMap[String, ColumnCapabilitiesImpl] = {
    schema.map { case (name, capabilities) => validateFieldName(name) -> capabilities }
  }

  def merge(schemaA: ListMap[String, ColumnCapabilitiesImpl], schemaB: ListMap[String, ColumnCapabilitiesImpl]): ListMap[String, ColumnCapabilitiesImpl] = {
    val result = mutable.LinkedHashMap[String, ColumnCapabilitiesImpl]()
    result ++= schemaA

    schemaB.foreach { case (name, capabilities) =>
      result += name -> ColumnCapabilitiesImpl.merge(capabilities, result.getOrElse(name, null), coercionLogic)
    }
    ListMap() ++ result
  }

  def druidToSpark(druidSchema: ListMap[String, ColumnCapabilitiesImpl]): StructType = {
    val structFields = druidSchema.map { case (name, capabilities) =>
      val basicSparkType = capabilities.getType match {
        case ValueType.FLOAT => FloatType
        case ValueType.LONG => LongType
        case ValueType.STRING => StringType
        case ValueType.DOUBLE => DoubleType
        case _ => throw new RuntimeException("Unsupported druid type: " + capabilities.getType)
      }

      val sparkType = if (capabilities.hasMultipleValues.isTrue) ArrayType(basicSparkType) else basicSparkType
      StructField(name, sparkType)
    }.toArray
    StructType(structFields)
  }

  private def validateFieldName(name: String): String = name.replaceAll("[\\s,;{}()=]", "")
}
