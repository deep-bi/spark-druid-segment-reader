package bi.deep.segments

import bi.deep.utils.Parsing
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.log4j.Logger
import org.joda.time.{Instant, Interval}

import scala.util.Try


case class Segment(path: Path, interval: Interval, version: Instant, partition: Int)


object Segment {
  implicit private val logger: Logger = Logger.getLogger("SegmentParser")

  private val pattern = """(\d+)""".r

  def apply(segment: SegmentVersion, status: LocatedFileStatus): Option[Segment] = Parsing.withLogger {
    val relative = status.getPath.toString.replace(segment.path.toString, "")
    for {
      partition <- Try(pattern.findFirstIn(relative).get.toInt)
    } yield {
      new Segment(status.getPath, segment.interval, segment.version, partition)
    }
  }
}