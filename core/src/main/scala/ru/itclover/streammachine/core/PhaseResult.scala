package ru.itclover.streammachine.core


import scala.reflect.ClassTag


sealed trait PhaseResult[+T] {

  def context: Map[Symbol, Any] = Map.empty

  def getValue(alias: Symbol): Option[Any] = context.get(alias)

  def map[B](f: T => B): PhaseResult[B]

  def flatMap[B](f: T => PhaseResult[B]): PhaseResult[B]

  def isTerminal: Boolean
}

object PhaseResult {

  sealed trait TerminalResult[+T] extends PhaseResult[T] {
    override def isTerminal: Boolean = true
  }


  case class Success[+T](t: T, override val context: Map[Symbol, Any]) extends TerminalResult[T] {
    override def map[B](f: T => B): PhaseResult[B] = Success(f(t), context)

    override def flatMap[B](f: T => PhaseResult[B]): PhaseResult[B] = f(t)
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

}
