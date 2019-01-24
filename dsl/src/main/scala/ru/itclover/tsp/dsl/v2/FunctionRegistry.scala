package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.v2.{Fail, Result, Succ}
import shapeless.{HList, HNil}

import scala.collection.mutable
import scala.reflect.ClassTag

/*class FunctionRegistry {
  private val functions: mutable.Map[(Symbol, Seq[ASTType]), Seq[Any] => Any] =
    mutable.Map.empty
  private val reducers: mutable.Map[(Symbol, ASTType), ((Any, Any) => Any, Any)] =
    mutable.Map.empty

  def registerFunction(name: Symbol, argTypes: Seq[ASTType], function: Seq[Any] => Any): Unit =
    functions((name, argTypes)) = function

  def getFunction(name: Symbol, argTypes: Seq[ASTType]): Option[Seq[Any] => Any] =
    functions.get((name, argTypes))

  def unregisterFunction(name: Symbol, argTypes: Seq[ASTType]): Unit =
    functions.remove((name, argTypes))

  def registerReducer(
    name: Symbol,
    argType: ASTType,
    function: (Any, Any) => Any,
    initial: Any
  ): Unit =
    reducers((name, argType)) = (function, initial)

  def getReducer(name: Symbol, argType: ASTType): Option[((Any, Any) => Any, Any)] =
    reducers.get((name, argType))

  def unregisterReducer(name: Symbol, argType: ASTType): Unit =
    reducers.remove((name, argType))

}

object FunctionRegistry {

  def createDefault: FunctionRegistry = {
    val fr = new FunctionRegistry
    fr.registerFunction(
      'add,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] + x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'sub,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] - x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'mul,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] * x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'div,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] / x(1).asInstanceOf[Double]
    )
    fr
  }
}*/

trait PFunction extends (Seq[Any] => Any)
trait PReducer extends ((Any, Any) => Any)

/**
  * Registry for runtime functions
  * Ensure that the result type of the function matches the corresponding ASTType. It's not automatic
  * @param functions Multi-argument functions (arguments wrapped into a Seq) and their return types
  * @param reducers Reducer functions, their return types and initial values
  */
case class FunctionRegistry(
  functions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)],
  reducers: Map[(Symbol, ASTType), (PReducer, ASTType, Any)]
) {

  def ++(other: FunctionRegistry) = FunctionRegistry(functions ++ other.functions, reducers ++ other.reducers)
}

object DefaultFunctions {

  val arithmeticFunctions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = Map(
    ('add, Seq(DoubleASTType, DoubleASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Double] + xs(1).asInstanceOf[Double], DoubleASTType)
    ),
    ('sub, Seq(DoubleASTType, DoubleASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Double] - xs(1).asInstanceOf[Double], DoubleASTType)
    ),
    ('mul, Seq(DoubleASTType, DoubleASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Double] * xs(1).asInstanceOf[Double], DoubleASTType)
    ),
    ('div, Seq(DoubleASTType, DoubleASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Double] / xs(1).asInstanceOf[Double], DoubleASTType)
    )
  )

  val logicalFunctions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = Map(
    ('and, Seq(BooleanASTType, BooleanASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] && xs(1).asInstanceOf[Boolean], BooleanASTType)
    ),
    ('or, Seq(BooleanASTType, BooleanASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] || xs(1).asInstanceOf[Boolean], BooleanASTType)
    ),
    ('xor, Seq(BooleanASTType, BooleanASTType)) -> (
      ((xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] ^ xs(1).asInstanceOf[Boolean], BooleanASTType)
    ),
    ('not, Seq(BooleanASTType)) -> (
      ((xs: Seq[Any]) => !xs(0).asInstanceOf[Boolean], BooleanASTType)
    )
  )

  val reducers: Map[(Symbol, ASTType), (PReducer, ASTType, Any)] = Map(
    ('sum, DoubleASTType) -> ((r: Any, x: Any) => r.asInstanceOf[Double] + x.asInstanceOf[Double], DoubleASTType, 0),
    ('count, DoubleASTType) -> ((r: Any, x: Any) => r.asInstanceOf[Double] + 1, DoubleASTType, 0)
  )
}

object DefaultFunctionRegistry
    extends FunctionRegistry(
      functions = DefaultFunctions.arithmeticFunctions ++ DefaultFunctions.logicalFunctions,
      reducers = DefaultFunctions.reducers
    )
