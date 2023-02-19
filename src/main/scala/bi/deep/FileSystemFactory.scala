package bi.deep

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.util.SerializableHadoopConfiguration

import java.net.URI


class FileSystemFactory(hadoopConf: SerializableHadoopConfiguration) extends Serializable {

  def fileSystemFor(path: String): FileSystem = {
    FileSystem.get(new URI(path), hadoopConf.config)
  }

  def fileSystemFor(path: Path): FileSystem = {
    FileSystem.get(path.toUri, hadoopConf.config)
  }
}