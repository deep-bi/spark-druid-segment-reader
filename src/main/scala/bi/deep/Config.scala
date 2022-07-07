package bi.deep

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.util.SerializableHadoopConfiguration

import java.time.LocalDate
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable


case class Config
(
  dataSource: String,
  startDate: LocalDate,
  endDate: LocalDate,
  factory: FileSystemFactory,
  druidTimestamp: String = Config.DEFAULT_DRUID_TIMESTAMP,
  inputPath: String,
  tempSegmentDir: String
)

object Config {

  val DEFAULT_DRUID_TIMESTAMP = ""
  val DEFAULT_TEMP_SEGMENT_DIR = ""
  val DRUID_SEGMENT_FILE = "index.zip"

  def getRequired(optionsMap: mutable.Map[String, String], fieldName: String): String = optionsMap.get(fieldName) match {
    case None => throw new RuntimeException(s"Field $fieldName is required, but was missing.")
    case Some(x) => x
  }

  def apply(options: DataSourceOptions, hadoopConfig: Configuration): Config = {
    val optionsMap = options.asMap().asScala
    new Config(
      dataSource = getRequired(optionsMap, "druid_data_source"),
      startDate = LocalDate.parse(getRequired(optionsMap, "start_date")),
      endDate = LocalDate.parse(getRequired(optionsMap, "end_date")),
      factory = new FileSystemFactory(SerializableHadoopConfiguration(hadoopConfig)),
      druidTimestamp = options.get("druid_timestamp").orElse(DEFAULT_DRUID_TIMESTAMP),
      inputPath = getRequired(optionsMap, "input_path"),
      tempSegmentDir = options.get("temp_segment_dir").orElse(DEFAULT_TEMP_SEGMENT_DIR)
    )
  }
}