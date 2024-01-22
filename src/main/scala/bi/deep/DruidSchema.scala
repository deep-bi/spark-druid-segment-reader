package bi.deep

import org.apache.druid.segment.column.ColumnCapabilitiesImpl

case class DruidSchema(
                        dimensions: Array[(String, ColumnCapabilitiesImpl)],
                        metrics: Array[(String, ColumnCapabilitiesImpl)]) {

  /** Validates the column names in the provided schema and removes all invalid characters, such as:
   * `,`, `;`, `{`, `}`, `(`, `)`, `=`, and all whitespace characters.
   * */
  def validateFields(): DruidSchema = {
    val newDimensions = dimensions.map { case (name, capabilities) => validateFieldName(name) -> capabilities }
    val newMetrics = metrics.map { case (name, capabilities) => validateFieldName(name) -> capabilities }
    DruidSchema(newDimensions, newMetrics)
  }

  private def validateFieldName(name: String): String = name.replaceAll("[\\s,;{}()=]", "")
}