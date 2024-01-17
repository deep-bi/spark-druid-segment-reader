package bi.deep

import org.apache.druid.segment.column.ColumnCapabilitiesImpl

class DruidSchema(
                   var dimensions: Array[(String, ColumnCapabilitiesImpl)],
                   var metrics: Array[(String, ColumnCapabilitiesImpl)]) {

  /** Validates the column names in the provided schema and removes all invalid characters, such as:
   * `,`, `;`, `{`, `}`, `(`, `)`, `=`, and all whitespace characters.
   * */

  def validateFields(): Unit = {
    dimensions.map { case (name, capabilities) => validateFieldName(name) -> capabilities }
    metrics.map { case (name, capabilities) => validateFieldName(name) -> capabilities }
  }

  private def validateFieldName(name: String): String = name.replaceAll("[\\s,;{}()=]", "")
}

object DruidSchema {
  def apply(
             dimensions: Array[(String, ColumnCapabilitiesImpl)],
             metrics: Array[(String, ColumnCapabilitiesImpl)]): DruidSchema = new DruidSchema(dimensions, metrics)
}
