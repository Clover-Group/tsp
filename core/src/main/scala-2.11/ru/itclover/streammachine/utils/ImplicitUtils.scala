package ru.itclover.streammachine.utils

import scala.util.{Failure, Success, Try}

object ImplicitUtils {
  implicit class RightBiasedEither[A, B](val e: Either[A, B]) extends AnyVal {
    def foreach[U](f: B => U): Unit = e.right.foreach(f)

    def map[C](f: B => C): Either[A, C] = e.right.map(f)

    def flatMap[C](f: B => Either[A, C]) = e.right.flatMap(f)

    def transform[AA, BB](fa: A => AA, fb: B => BB): Either[AA, BB] = e.left.map[AA](fa).map[BB](fb)

    def transformLeft[C](lTransform: A => C) = e.left.map(lTransform)

    def getOrElse[BB >: B](value: => BB): BB = e.right.getOrElse(value)
  }

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
}
