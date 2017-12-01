package ru.itclover.streammachine.core

sealed trait PhaseResult[+T] {
  def map[B](f: T => B): PhaseResult[B]
}

object PhaseResult {

  sealed trait TerminalResult[+T] extends PhaseResult[T]

  case class Success[T](t: T) extends TerminalResult[T] {
    override def map[B](f: T => B): PhaseResult[B] = Success(f(t))
  }

  case class Failure(msg: String) extends TerminalResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = this
  }

  case object Stay extends PhaseResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = Stay
  }

}
