package bi.deep.segments

import bi.deep.utils.{Parsing, TimeUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.Logger
import org.joda.time.Interval


/** A path to Segment's Interval */
case class SegmentInterval(path: Path, interval: Interval) {
  def overlapedBy(range: Interval): Boolean = range.overlaps(interval)
}

object SegmentInterval {
  implicit private val logger: Logger = Logger.getLogger("SegmentIntervalParser")

  def apply(status: FileStatus): Option[SegmentInterval] = Parsing.withLogger {
    for {
      interval <- TimeUtils.parseIntervalFromHadoop(status.getPath.getName)
    } yield new SegmentInterval(status.getPath, interval)
  }
}