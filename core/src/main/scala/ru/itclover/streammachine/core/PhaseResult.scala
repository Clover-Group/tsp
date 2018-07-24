package ru.itclover.streammachine.core

sealed trait PhaseResult[+T] {
  def map[B](f: T => B): PhaseResult[B]

  def flatMap[B](f: T => PhaseResult[B]): PhaseResult[B]

  def isTerminal: Boolean

  def getOrElse[B >: T](other: => B) = other
}

object PhaseResult {

  sealed trait TerminalResult[+T] extends PhaseResult[T] {
    override def isTerminal: Boolean = true
  }

  case class Success[+T](t: T) extends TerminalResult[T] {
    override def map[B](f: T => B): PhaseResult[B] = Success(f(t))

    override def flatMap[B](f: T => PhaseResult[B]): PhaseResult[B] = f(t)

    override def getOrElse[B >: T](other: => B) = t
  }

  case class Failure(msg: String) extends TerminalResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = this

    override def flatMap[B](f: Nothing => PhaseResult[B]): PhaseResult[B] = this
  }

  case object Stay extends PhaseResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = Stay

    override def isTerminal: Boolean = false

    override def flatMap[B](f: Nothing => PhaseResult[B]): PhaseResult[B] = this
  }

  val heartbeat = Failure("__heartbeat__")

}
