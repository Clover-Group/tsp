package ru.itclover.streammachine.core

sealed trait PhaseResult[+T]{
  def map[B](f: T => B): PhaseResult[B]
}

object PhaseResult {

  case class Success[T](t: T) extends PhaseResult[T] {
    override def map[B](f: T => B): PhaseResult[B] = Success(f(t))
  }

  case class Failure(msg: String) extends PhaseResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = this
  }

  case object Stay extends PhaseResult[Nothing] {
    override def map[B](f: Nothing => B): PhaseResult[B] = Stay
  }

}
