package bi.deep

import bi.deep.Config.DRUID_SEGMENT_FILE
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Key
import org.apache.commons.codec.digest.DigestUtils.sha1Hex
import org.apache.commons.io.FileUtils
import org.apache.druid.common.config.NullHandling
import org.apache.druid.guice.annotations.Json
import org.apache.druid.guice.{GuiceInjectableValues, GuiceInjectors}
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.java.util.emitter.EmittingLogger
import org.apache.druid.query.DruidProcessingConfig
import org.apache.druid.segment.IndexIO
import org.apache.druid.segment.column.ColumnConfig
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files


trait DruidSegmentReader {

  DruidSegmentReader.init()

  def indexIO: IndexIO = {
    // https://www.tabnine.com/code/java/classes/org.apache.druid.guice.GuiceInjectors
    val injector = GuiceInjectors.makeStartupInjector()
    val injectables = new GuiceInjectableValues(injector)

    // Mark as ERROR level to not display warnings with unreadable segment
    Logger.getLogger(classOf[IndexIO]).setLevel(Level.ERROR)

    val mapper = injector
      .getInstance(Key.get(classOf[ObjectMapper], classOf[Json]))
      .setInjectableValues(injectables)

    new IndexIO(mapper,
      new DruidProcessingConfig {
        override def getFormatString: String = "processing-%s"
      }
    )
  }

  def withSegment[R](file: String, config: Config, path: String)(handler: File => R): R = {
    val fileSystem = config.factory.fileSystemFor(path)

    val segmentDir = if (config.tempSegmentDir != "") {
      val temp = new File(config.tempSegmentDir, sha1Hex(file))
      FileUtils.forceMkdir(temp)
      temp
    } else {
      Files.createTempDirectory("segments" + sha1Hex(file)).toFile
    }

    try {
      val segmentFile = new File(segmentDir, DRUID_SEGMENT_FILE)
      fileSystem.copyToLocalFile(new Path(file), new Path(segmentFile.toURI))
      ZipUtils.unzip(segmentFile, segmentDir)
      handler(segmentDir)
    }
    finally {
      FileUtils.deleteDirectory(segmentDir)
    }
  }
}

object DruidSegmentReader {
  private val initiated: Boolean = false

  def init(): Unit = {
    if (!initiated) {
      NullHandling.initializeForTests()
    }
  }
}
