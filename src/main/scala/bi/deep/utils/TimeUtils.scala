package bi.deep.utils

import org.joda.time.{Instant, Interval}

import scala.util.Try

object TimeUtils {

  def parseInstantFromHadoop(value: String): Try[Instant] = Try(Instant.parse(value.replace('_', ':')))

  def parseIntervalFromHadoop(value: String): Try[Interval] = for {
    start <- parseInstantFromHadoop(value.substring(0, 24))
    end <- parseInstantFromHadoop(value.substring(25))
  } yield new Interval(start, end)

}
