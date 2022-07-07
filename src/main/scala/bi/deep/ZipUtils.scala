package bi.deep

import org.apache.commons.io.FileUtils

import java.io.File
import java.util.zip.ZipFile
import scala.collection.JavaConverters._


object ZipUtils {

  def unzip(inputZipFile: File, outputDirectory: File): Unit = {
    val zipFile = new ZipFile(inputZipFile)
    zipFile.entries().asScala.foreach { e =>
      FileUtils.copyInputStreamToFile(zipFile.getInputStream(e), new File(outputDirectory, e.getName))
    }
  }
}