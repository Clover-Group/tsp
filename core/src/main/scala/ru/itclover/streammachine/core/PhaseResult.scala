package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.AliasedParser.Aliased

import scala.reflect.ClassTag


sealed trait PhaseResult[+T] {
  def map[B](f: T => B): PhaseResult[B]

  def flatMap[B](f: T => PhaseResult[B]): PhaseResult[B]

  def isTerminal: Boolean
}

object PhaseResult {

//  def findResultByName[T: ClassTag](result: PhaseResult[T], alias: String): Option[PhaseResult[T]] = {
//    val clazz = implicitly[ClassTag[T]].runtimeClass
//    result match {
//      case Success(Aliased(al, inner)) if al == alias => Some(inner)
//      case Success(other) => findResultByName(other, alias)
//
//      case AliasedSuccess(innerT: PhaseResult[T], someAlias: String) if clazz.isInstance(innerT) =>
//        if (someAlias == alias) Some(innerT)
//        else findResultByName(innerT, alias)
//      case _ => None
//    }
//  }

  sealed trait TerminalResult[+T] extends PhaseResult[T] {
    override def isTerminal: Boolean = true
  }


  case class Success[+T](t: T) extends TerminalResult[T] {
    override def map[B](f: T => B): PhaseResult[B] = Success(f(t))

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
