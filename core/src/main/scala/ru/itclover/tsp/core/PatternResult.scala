package ru.itclover.tsp.core

sealed trait PatternResult[+T] extends Product with Serializable {
  def map[B](f: T => B): PatternResult[B]

  def flatMap[B](f: T => PatternResult[B]): PatternResult[B]

  def isTerminal: Boolean

  def getOrElse[B >: T](other: => B) = other
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

}
