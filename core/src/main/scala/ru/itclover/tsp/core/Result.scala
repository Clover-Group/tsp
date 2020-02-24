package ru.itclover.tsp.core

sealed trait Result[+A] {
  private def get: A = this match {
    case Fail    => throw new RuntimeException("Illegal get on Fail")
    case Succ(t) => t
  }

  def isFail: Boolean = this match {
    case Fail    => true
    case Succ(_) => false
  }

  def isSuccess: Boolean = !isFail

  @inline final def map[B](f: A => B): Result[B] =
    if (isFail) Fail else Succ(f(this.get))

  @inline final def fold[B](ifEmpty: => B)(f: A => B): B =
    if (isFail) ifEmpty else f(this.get)

  @inline final def flatMap[B](f: A => Result[B]): Result[B] =
    if (isFail) Fail else f(this.get)

  @inline final def getOrElse[B >: A](default: => B): B =
    if (isFail) default else this.get

  @inline final def foreach(f: A => Unit): Unit = if (isSuccess) f(this.get)
}

object Result {
  implicit class OptionToResult[T](private val opt: Option[T]) extends AnyVal {

    def toResult: Result[T] = opt match {
      case None    => Fail
      case Some(t) => Succ(t)
    }
  }

  def fail[T]: Result[T] = Fail
  def succ[T](t: T): Result[T] = Succ(t)
  val succUnit: Result[Unit] = Succ(())
}

case class Succ[T](t: T) extends Result[T]

object Fail extends Result[Nothing]{
  override def toString: String = "Fail"
}
