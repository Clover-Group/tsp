package ru.itclover.tsp.dsl

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.Intervals.TimeInterval
import ru.itclover.tsp.core.Window
import UtilityTypes.ParseException

import scala.reflect.ClassTag

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ASTTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val funReg: DefaultFunctionRegistry.type = DefaultFunctionRegistry

  //TODO: no refactoring in loop compare in case of class derivation
  "AST types" should "correctly construct from Scala types" in {
    ASTType.of[Int] shouldBe IntASTType
    ASTType.of[java.lang.Integer] shouldBe IntASTType
    ASTType.of[Long] shouldBe LongASTType
    ASTType.of[java.lang.Long] shouldBe LongASTType
    ASTType.of[Boolean] shouldBe BooleanASTType
    ASTType.of[java.lang.Boolean] shouldBe BooleanASTType
    ASTType.of[Double] shouldBe DoubleASTType
    ASTType.of[java.lang.Double] shouldBe DoubleASTType
    ASTType.of[String] shouldBe StringASTType
    ASTType.of[List[Int]] shouldBe AnyASTType
  }

  //TODO: no refactoring in loop compare in case of class derivation
  "AST types" should "correctly determine" in {
    Constant(1.0).valueType shouldBe DoubleASTType
    Constant(1L).valueType shouldBe LongASTType
    Constant(true).valueType shouldBe BooleanASTType
    Constant(List(1, 2, 3)).valueType shouldBe AnyASTType
  }

  //TODO: no refactoring in loop compare in case of class derivation
  "Identifiers" should "have correct types" in {
    Identifier('intVar, ClassTag.Int).valueType shouldBe IntASTType
    Identifier('longVar, ClassTag.Long).valueType shouldBe LongASTType
    Identifier('boolVar, ClassTag.Boolean).valueType shouldBe BooleanASTType
    Identifier('doubleVar, ClassTag.Double).valueType shouldBe DoubleASTType
    Identifier('stringVar, ClassTag(classOf[String])).valueType shouldBe StringASTType
  }

  //TODO: no refactoring in loop compare in case of class derivation
  "AST operations" should "require types" in {
    FunctionCall('and, Seq(Constant(true), Constant(false))).valueType shouldBe BooleanASTType
    a[ParseException] should be thrownBy FunctionCall('and, Seq(Constant(true))) // only 1 argument
    a[ParseException] should be thrownBy FunctionCall('and, Seq(Constant(true), Constant("false"))) // invalid types
  }

  "Windowed operators" should "construct correctly" in {
    val winOp = ForWithInterval(
      FunctionCall('gt, Seq(Identifier('sensor, ClassTag.Double), Constant(0))),
      Some(false),
      Window(60000),
      TimeInterval(0, 10000)
    )
    winOp.valueType shouldBe BooleanASTType
    winOp.metadata shouldBe PatternMetadata(Set('sensor), 60000)
  }

  "Type requirements" should "be met" in {
    noException should be thrownBy Constant(10L).requireType(LongASTType, "Type was not long")
    a[ParseException] should be thrownBy Constant(10L).requireType(BooleanASTType, "Type was not boolean")
  }

  "Aggregate functions" should "be correctly created from symbols" in {
    AggregateFn.fromSymbol('sum) shouldBe Sum
    AggregateFn.fromSymbol('avg) shouldBe Avg
    AggregateFn.fromSymbol('count) shouldBe Count
    AggregateFn.fromSymbol('lag) shouldBe Lag
    a[ParseException] should be thrownBy AggregateFn.fromSymbol('invalid)
  }

  "Range" should "be correctly constructed" in {
    val range = Range(10000L, 60000L)
    range.metadata shouldBe PatternMetadata.empty
    range.valueType shouldBe LongASTType
  }

}
