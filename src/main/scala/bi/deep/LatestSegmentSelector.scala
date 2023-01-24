package bi.deep

import bi.deep.Config.DRUID_SEGMENT_FILE
import bi.deep.HadoopUtils.remoteIterator
import bi.deep.LatestSegmentSelector.{getIntervals, listPrimaryPartitions}
import org.apache.hadoop.fs.{LocatedFileStatus, Path}

import java.time.LocalDate


case class LatestSegmentSelector(config: Config, mainPathName: String) {

  private val directoryPaths = listPrimaryPartitions(getIntervals(config.startDate, config.endDate), mainPathName)
  private var results: Array[LocatedFileStatus] = Array.empty[LocatedFileStatus]

  private def latestDate(file: LocatedFileStatus, path: String): String = {
    file.toString.replace(path, "").split('/')(1)
  }

  protected def lastCreatedDir(files: List[LocatedFileStatus], path: String): String = {
    latestDate(files.maxBy(latestDate(_, path)), path)
  }

  def createPathsList(path: String): List[LocatedFileStatus] = {
    val files = config.factory.fileSystemFor(path)
      .listFiles(new Path(path), true)
      .toList
    files.filter(p => p.getPath.getName == DRUID_SEGMENT_FILE)
  }

  def latestSegmentForPath(path: String): Unit = {
    val indexFiles = createPathsList(path)
    val lastDate = lastCreatedDir(indexFiles, path)
    val latestFiles = createPathsList(path + "/" + lastDate)
    results = results ++ latestFiles
  }

  def getPathsArray: Array[LocatedFileStatus] = {
    directoryPaths.foreach(latestSegmentForPath)
    results
  }
}

object LatestSegmentSelector {

  def getIntervals(startDate: LocalDate, endDate: LocalDate): List[(LocalDate, LocalDate)] = {
    Stream.iterate(startDate)(_.plusDays(1)).takeWhile(_.isBefore(endDate))
      .map(date => (date, date.plusDays(1))).toList
  }

  def listPrimaryPartitions(dates: List[(LocalDate, LocalDate)], path: String): List[String] = {
    path.split(':')(0) match {
      case "hdfs" => dates.map(x => path + s"${x._1}T00_00_00.000Z_${x._2}T00_00_00.000Z")
      case _ => dates.map(x => path + s"${x._1}T00:00:00.000Z_${x._2}T00:00:00.000Z")
    }
  }
}