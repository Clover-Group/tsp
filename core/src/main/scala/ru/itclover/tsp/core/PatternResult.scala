package ru.itclover.tsp.core
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}

sealed trait PatternResult[+T] extends Product with Serializable {
  def map[B](f: T => B): PatternResult[B]

  def flatMap[B](f: T => PatternResult[B]): PatternResult[B]

  def isTerminal: Boolean

  def getOrElse[B >: T](other: => B) = other

  @inline final def fold[B](ifEmpty: => B)(f: T => B): B =
    this match {
      case Success(t) => f(t)
      case _          => ifEmpty
    }
}

object PatternResult {

  sealed trait TerminalResult[+T] extends PatternResult[T] {
    override def isTerminal: Boolean = true
  }

  case class Success[+T](t: T) extends TerminalResult[T] {
    override def map[B](f: T => B): PatternResult[B] = Success(f(t))

    override def flatMap[B](f: T => PatternResult[B]): PatternResult[B] = f(t)

    override def getOrElse[B >: T](other: => B) = t
  }

  object Success {

    val unitSuccess = Success(())

    def of(x: Unit): Success[Unit] = unitSuccess

    def of[T](t: T): Success[T] = Success(t)

  }

  case class Failure(msg: String) extends TerminalResult[Nothing] {
    override def map[B](f: Nothing => B): PatternResult[B] = this

    override def flatMap[B](f: Nothing => PatternResult[B]): PatternResult[B] = this
  }

  case object Stay extends PatternResult[Nothing] {
    override def map[B](f: Nothing => B): PatternResult[B] = Stay

    override def isTerminal: Boolean = false

    override def flatMap[B](f: Nothing => PatternResult[B]): PatternResult[B] = this
  }

  val heartbeat = Failure("__heartbeat__")

  def fromOption[T](opt: Option[T]): PatternResult[T] = opt match {
    case Some(x) => Success(x)
    case None    => Stay
  }
}
