package bi.deep.utils

import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Parsing {

  def withLogger[T](body: => Try[T])(implicit logger: Logger): Option[T] = {
    body match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        logger.warn(s"Failed to parse segment interval: ${exception.getMessage}", exception)
        None
    }
  }

}
