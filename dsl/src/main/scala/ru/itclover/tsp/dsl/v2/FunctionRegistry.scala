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

case class FunctionRegistry(
  functions: Map[(Symbol, Seq[ASTType]), Seq[Any] => Any],
  reducers: Map[(Symbol, ASTType), ((Any, Any) => Any, Any)]
)