package bi.deep.utils

import org.apache.hadoop.fs.RemoteIterator

object Extensions {

  implicit class RemoteIteratorExt[T](val iterator: RemoteIterator[T]) extends AnyVal {

    def asScala: Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = iterator.hasNext

      override def next(): T = iterator.next()
    }

  }

}
