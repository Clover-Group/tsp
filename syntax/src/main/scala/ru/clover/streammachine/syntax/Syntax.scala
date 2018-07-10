package ru.clover.streammachine.syntax

import cats.Eval
import parseback.{LineStream, ParseError}
import parseback.util.{Catenable, EitherSyntax}
import parseback.compat.cats._
import ru.itclover.streammachine.core.{PhaseParser, Time}
import ru.itclover.streammachine.core.Time.TimeExtractor
import shims.syntax.either

object Syntax extends App {

  def createParser[Event: TimeExtractor](phaseString: String): Eval[either.\/[List[ParseError], Catenable[PhaseParser[Event, _, _]]]] = {

    val input = LineStream[Eval](phaseString)

    val parser = new SyntaxParser[Event]()
    parser.parseback(input)

  }
}
