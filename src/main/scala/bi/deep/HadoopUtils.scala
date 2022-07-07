package bi.deep

import org.apache.hadoop.fs.RemoteIterator

import scala.language.implicitConversions


object HadoopUtils {

  implicit def remoteIterator[E](remoteIterator: RemoteIterator[E]): Iterator[E] = {
    new Iterator[E] {
      override def hasNext: Boolean = remoteIterator.hasNext

      override def next(): E = remoteIterator.next()
    }
  }
}