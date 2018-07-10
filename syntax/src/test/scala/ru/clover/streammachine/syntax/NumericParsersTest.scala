package ru.clover.streammachine.syntax

import org.scalatest.WordSpec
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.core.PhaseResult.Stay
import ru.itclover.streammachine.core.{PhaseParser, TestPhase, TestingEvent}
//import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
//import ru.itclover.streammachine.core.Time._
//import ru.itclover.streammachine.phases.ConstantPhases.ConstantFunctions
//import ru.itclover.streammachine.phases.NumericPhases.SymbolParser
import ru.itclover.streammachine.phases.NumericPhases._
import ru.itclover.streammachine.phases.{ConstantPhases, NoState}
import ru.itclover.streammachine.utils.ParserMatchers

import scala.Predef.{any2stringadd => _}


class NumericParsersTest extends WordSpec with ParserMatchers {

  "BinaryNumericParser" should {
    "work on stay and success events and +, -, *, /" in {
      val b: NumericPhaseParser[TestingEvent[Double], NoState] = ConstantPhases[TestingEvent[Double], Double](10.0)
      checkOnTestEvents(
        (p: TestPhase[Double]) => Syntax.createParser("p + b").value.right.get,
        staySuccesses,
        Seq(Success(11.0), Success(11.0), Success(12.0), Success(12.0), Success(11.0), Success(13.0), Failure("Test"), Success(14.0))
      )

      checkOnTestEvents(
        (p: TestPhase[Double]) => p - b,
        staySuccesses,
        Seq(Success(-9.0), Success(-9.0), Success(-8.0), Success(-8.0), Success(-9.0), Success(-7.0), Failure("Test"), Success(-6.0))
      )

      checkOnTestEvents(
        (p: TestPhase[Double]) => p * b,
        staySuccesses,
        Seq(Success(10.0), Success(10.0), Success(20.0), Success(20.0), Success(10.0), Success(30.0), Failure("Test"), Success(40.0))
      )

      checkOnTestEvents(
        (p: TestPhase[Double]) => p / b,
        staySuccesses,
        Seq(Success(0.1), Success(0.1), Success(0.2), Success(0.2), Success(0.1), Success(0.3), Failure("Test"), Success(0.4))
      )
    }
  }

  "Numeric parsers" should {
    "work for type casting" in {
      import ru.itclover.streammachine.phases.NumericPhases.{SymbolParser, _}

      val intVal = 18
      val floatVal = 18.0f
      val strVal = "18"
      val longVal = 18L

      implicit val fixedIntSymbolExtractor = new SymbolExtractor[TestingEvent[Double], Int] {
        override def extract(event: TestingEvent[Double], symbol: Symbol) = intVal
      }
      implicit val fixedFloatSymbolExtractor = new SymbolExtractor[TestingEvent[Double], Float] {
        override def extract(event: TestingEvent[Double], symbol: Symbol) = floatVal
      }
      implicit val fixedStringSymbolExtractor = new SymbolExtractor[TestingEvent[Double], String] {
        override def extract(event: TestingEvent[Double], symbol: Symbol) = strVal
      }
      implicit val fixedLongSymbolExtractor = new SymbolExtractor[TestingEvent[Double], Long] {
        override def extract(event: TestingEvent[Double], symbol: Symbol) = longVal
      }

      val intParser: PhaseParser[TestingEvent[Double], NoState, Int] = 'i.as[Int]
      val floatParser: PhaseParser[TestingEvent[Double], NoState, Float] = 'i.as[Float]
      val strParser: PhaseParser[TestingEvent[Double], NoState, String] = 'i.as[String]
      val longParser: PhaseParser[TestingEvent[Double], NoState, Long] = 'i.as[Long]


      checkOnTestEvents(
        (p: TestPhase[Double]) => p.flatMap(_ => intParser),
        staySuccesses,
        Seq(Success(intVal), Success(intVal), Success(intVal), Success(intVal), Success(intVal), Success(intVal), Failure("Test"), Success(intVal))
      )

      checkOnTestEvents(
        (p: TestPhase[Double]) => p.flatMap(_ => floatParser),
          staySuccesses,
          Seq(Success(floatVal), Success(floatVal), Success(floatVal), Success(floatVal), Success(floatVal), Success(floatVal), Failure("Test"), Success(floatVal)),
          epsilon = Some(0.001f)
      )


      checkOnTestEvents_strict(
        (p: TestPhase[Double]) => p.flatMap(_ => strParser),
          staySuccesses,
          Seq(Success(strVal), Success(strVal), Success(strVal), Success(strVal), Success(strVal), Success(strVal), Failure("Test"), Success(strVal))
      )

      checkOnTestEvents(
        (p: TestPhase[Double]) => p.flatMap(_ => longParser),
        staySuccesses,
        Seq(Success(longVal), Success(longVal), Success(longVal), Success(longVal), Success(longVal), Success(longVal), Failure("Test"), Success(longVal))
      )
    }
  }

  "abs parser" should {
    "work" in {
      import ru.itclover.streammachine.core.PhaseParser.Functions._
      val results = Stay :: Success(-1.0) :: Success(1.0) :: Failure("Test") :: Nil
      checkOnTestEvents(
        (p: TestPhase[Double]) => abs(p),
        for((t, res) <- times.take(results.length).zip(results)) yield TestingEvent(res, t),
        Seq(Success(1.0), Success(1.0), Success(1.0), Failure("Test"))
      )
    }
  }


}
