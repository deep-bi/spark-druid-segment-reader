package bi.deep.segments

import bi.deep.utils.{Parsing, TimeUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.Logger
import org.joda.time.{Instant, Interval}


case class SegmentVersion(path: Path, interval: Interval, version: Instant)

object SegmentVersion {
  implicit private val logger: Logger = Logger.getLogger("SegmentVersionParser")
  def apply(segment: SegmentInterval, status: FileStatus): Option[SegmentVersion] = Parsing.withLogger {
    for {
      version <- TimeUtils.parseInstantFromHadoop(status.getPath.getName)
    } yield new SegmentVersion(status.getPath, segment.interval, version)
  }
}