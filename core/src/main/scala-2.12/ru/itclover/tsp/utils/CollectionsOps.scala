package ru.itclover.tsp.utils

import java.util.regex.Pattern
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object CollectionsOps {
  implicit class TryOps[T](val t: Try[T]) extends AnyVal {
    def eventually[Ignore](effect: => Ignore): Try[T] = t.transform(_ => { effect; t }, _ => { effect; t })

    def toEither: Either[Throwable, T] = t match {
      case Success(some) => Right(some)
      case Failure(err) => Left(err)
    }

    def asEitherString: Either[String, T] = t match {
      case Success(some) => Right(some)
      case Failure(err) => Left(err.getMessage)
    }
  }


  implicit class OptionOps[T](val o: Option[T]) extends AnyVal {
    def toEither[L](whenNone: => L): Either[L, T] = o match {
      case Some(s) => Right(s)
      case None => Left(whenNone)
    }

    def toFuture[L](whenNone: => Throwable): Future[T] = o match {
      case Some(s) => Future.successful(s)
      case None => Future.failed(whenNone)
    }

    def toTry(whenNone: => Throwable): Try[T] = o match {
      case Some(s) => Success(s)
      case None => Failure(whenNone)
    }
  }


  implicit class StringOps(val s: String) extends AnyVal {
    def replaceLast(regex: String, replacement: String, patternFlags: Int = 0) = {
      Pattern.compile("(?s)(.*)" + regex, patternFlags).matcher(s).replaceFirst("$1" + replacement)
    }
  }


  implicit class MutableQueueOps[A](val queue: mutable.Queue[A]) extends AnyVal {

    /** Hot-spot!
      * Dequeue first elements while predicate succeeding and don'd bother with rest of queue
      * once predicate become `false` (in opposite to [[mutable.Queue.dequeueAll]]).
      * @return seq of dropped elements
      */
    def dequeueWhile(predicate: A => Boolean): Seq[A] = {
      val deqList = mutable.ArrayBuffer.empty[A]
      while (queue.nonEmpty && predicate(queue.head)) {
        deqList.append(queue.dequeue())
      }
      deqList
    }
  }
}
