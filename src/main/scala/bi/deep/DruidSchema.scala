package bi.deep

import org.apache.druid.segment.column.ColumnCapabilitiesImpl

class DruidSchema private(
                           val dimensions: Array[(String, ColumnCapabilitiesImpl)],
                           val metrics: Array[(String, ColumnCapabilitiesImpl)])

object DruidSchema {
  def apply(
             dimensions: Array[(String, ColumnCapabilitiesImpl)],
             metrics: Array[(String, ColumnCapabilitiesImpl)]): DruidSchema =
    new DruidSchema(dimensions.map(fixFieldName), metrics.map(fixFieldName))

  /** Fixes the column names in the provided schema and removes all invalid characters, such as:
   * `,`, `;`, `{`, `}`, `(`, `)`, `=`, and all whitespace characters.
   * */
  private def fixFieldName(pair: (String, ColumnCapabilitiesImpl)): (String, ColumnCapabilitiesImpl) = pair match {
    case (name, capabilities) => name.replaceAll("[\\s,;{}()=]", "") -> capabilities
  }
}