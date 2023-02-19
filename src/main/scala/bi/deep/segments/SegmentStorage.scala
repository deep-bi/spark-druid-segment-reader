package bi.deep.segments

import bi.deep.segments.MixedGranularitySolver.Overlapping
import bi.deep.utils.Extensions.RemoteIteratorExt
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.joda.time.Interval

import java.time.{LocalDate, ZoneId}

case class SegmentStorage(root: Path) {

  import SegmentStorage._

  private val logger = Logger.getLogger(classOf[SegmentStorage])

  def findIntervals(start: LocalDate, end: LocalDate)(implicit fs: FileSystem): List[SegmentInterval] = {

    val startTime = start.atStartOfDay(ZoneId.of("UTC")).toInstant
    val endTime = end.atStartOfDay(ZoneId.of("UTC")).toInstant
    val range = new Interval(startTime.toEpochMilli, endTime.toEpochMilli)

    fs.listStatusIterator(root).asScala
      .flatMap(SegmentInterval(_))
      .filter(_.overlapedBy(range))
      .toList
  }

  def findLatestVersion(segmentInterval: SegmentInterval)(implicit fs: FileSystem): SegmentVersion = {
    fs.listStatusIterator(segmentInterval.path).asScala
      .flatMap(SegmentVersion(segmentInterval, _))
      .maxBy(_.version.getMillis)
  }

  def findPartitions(segmentVersion: SegmentVersion)(implicit fs: FileSystem): List[Segment] = {
    fs.listFiles(segmentVersion.path, true).asScala
      .flatMap(Segment(segmentVersion, _))
      .toList
  }

  def findValidSegments(start: LocalDate, end: LocalDate)(implicit fs: FileSystem): List[Segment] = {
    val versions = findIntervals(start, end)
      .map(findLatestVersion)

    val segments = MixedGranularitySolver
      .solve(versions)
      .flatMap(findPartitions)

    logger.info(s"Found ${segments.length} segments in range [$start, $end]")
    segments
  }

}

object SegmentStorage {

  implicit val segmentOverlapping: Overlapping[SegmentVersion] = new Overlapping[SegmentVersion] {
    /** Defines if two values overlaps (have common part) */
    override def overlaps(left: SegmentVersion, right: SegmentVersion): Boolean = {
      left.interval.overlaps(right.interval)
    }
  }

  implicit val segmentOrdering: Ordering[SegmentVersion] = new Ordering[SegmentVersion] {
    override def compare(left: SegmentVersion, right: SegmentVersion): Int = left.version.compareTo(right.version)
  }

}