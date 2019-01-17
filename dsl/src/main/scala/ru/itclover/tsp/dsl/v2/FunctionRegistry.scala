package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.v2.{Fail, Result, Succ}
import shapeless.{HList, HNil}

import scala.collection.mutable
import scala.reflect.ClassTag

//class FunctionRegistry {
//  private val functions: mutable.Map[(Symbol, Class[_], Class[_]), Function[_, _]] =
//    mutable.Map.empty
//
//  def registerFunction[H <: HList, R](name: Symbol, function: Function[H, R])
//                                     (implicit cth: ClassTag[H], ctr: ClassTag[R]): Unit =
//    functions((name, cth.runtimeClass, ctr.runtimeClass)) = function
//  def getFunction[H <: HList, R](name: Symbol)
//                                (implicit cth: ClassTag[H], ctr: ClassTag[R]): Option[Function[H, R]] =
//    functions.get((name, cth.runtimeClass, ctr.runtimeClass)).asInstanceOf[Option[Function[H, R]]]
//  def unregisterFunction[H <: HList, R](name: Symbol)
//                                       (implicit cth: ClassTag[H], ctr: ClassTag[R]): Unit =
//    functions.remove((name, cth.runtimeClass, ctr.runtimeClass))
//}
//
//
//object FunctionRegistry {
//  def createDefault: FunctionRegistry = {
//    val fr = new FunctionRegistry
//    //fr.registerFunction('add, (x: Double :: Double :: HNil) => x(0) + x(1))
//    //fr.registerFunction('sub, (x: Double :: Double :: HNil) => x(0) - x(1))
//    //fr.registerFunction('mul, (x: Double :: Double :: HNil) => x(0) * x(1))
//    //fr.registerFunction('div, (x: Double :: Double :: HNil) => x(0) / x(1))
//    fr
//  }
//}

// A simpler version of FunctionRegistry

class FunctionRegistry {
  private val functions1: mutable.Map[(Symbol, Class[_], Class[_]), _ => Result[_]] =
    mutable.Map.empty

  private val functions2: mutable.Map[(Symbol, Class[_], Class[_], Class[_]), (_, _) => Result[_]] =
    mutable.Map.empty

  private val reducers: mutable.Map[(Symbol, Class[_], Class[_]), ((_, _) => Result[_], _)] =
    mutable.Map.empty

  private val aliases: mutable.Map[Symbol, Symbol] = mutable.Map.empty

  def registerAlias(alias: Symbol, name: Symbol): Unit = aliases(alias) = name

  def unregisterAlias(alias: Symbol): Option[Symbol] = aliases.remove(alias)

  def getAliasedName(alias: Symbol): Symbol = aliases.getOrElse(alias, alias)

  def registerFunction1[T1, T2](
    name: Symbol,
    function: T1 => Result[T2]
  )(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Unit =
    functions1((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass)) = function

  def getFunction1[T1, T2](name: Symbol)(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Option[T1 => Result[T2]] =
    functions1.get((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass)).asInstanceOf[Option[T1 => Result[T2]]]

  def unregisterFunction1[T1, T2](name: Symbol)(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Unit =
    functions1.remove((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass))

  def registerFunction2[T1, T2, T3](
    name: Symbol,
    function: (Result[T1], Result[T2]) => Result[T3]
  )(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2], ctt3: ClassTag[T3]): Unit =
    functions2((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass, ctt3.runtimeClass)) = function

  def getFunction2[T1, T2, T3](
    name: Symbol
  )(
    implicit ctt1: ClassTag[T1],
    ctt2: ClassTag[T2],
    ctt3: ClassTag[T2]
  ): Option[(Result[T1], Result[T2]) => Result[T3]] =
    functions2
      .get((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass, ctt3.runtimeClass))
      .asInstanceOf[Option[(Result[T1], Result[T2]) => Result[T3]]]

  def unregisterFunction2[T1, T2, T3](
    name: Symbol
  )(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2], ctt3: ClassTag[T3]): Unit =
    functions2.remove((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass, ctt3.runtimeClass))

  def registerReducer[T1, T2](
    name: Symbol,
    function: (Result[T2], Result[T1]) => Result[T2],
    initial: T2
  )(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Unit =
    reducers((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass)) = (function, initial)

  def getReducer[T1, T2](
    name: Symbol
  )(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Option[((Result[T2], Result[T1]) => Result[T2], T2)] =
    reducers
      .get((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass))
      .asInstanceOf[Option[((Result[T2], Result[T1]) => Result[T2], T2)]]

  def unregisterReducer[T1, T2](name: Symbol)(implicit ctt1: ClassTag[T1], ctt2: ClassTag[T2]): Unit =
    reducers.remove((getAliasedName(name), ctt1.runtimeClass, ctt2.runtimeClass))
}

object FunctionRegistry {

  private def coupleResults[T1, T2, T3](r1: Result[T1], r2: Result[T2], f: (T1, T2) => T3): Result[T3] = {
    (r1, r2) match {
      case (Succ(t1), Succ(t2)) => Succ(f(t1, t2))
      case _                    => Fail
    }
  }

  def createDefault: FunctionRegistry = {
    val fr = new FunctionRegistry

    // Arithmetic operations
    fr.registerFunction2[Double, Double, Double]('add, (x, y) => coupleResults[Double, Double, Double](x, y, _ + _))
    fr.registerFunction2[Double, Double, Double]('sub, (x, y) => coupleResults[Double, Double, Double](x, y, _ - _))
    fr.registerFunction2[Double, Double, Double]('mul, (x, y) => coupleResults[Double, Double, Double](x, y, _ * _))
    fr.registerFunction2[Double, Double, Double]('div, (x, y) => coupleResults[Double, Double, Double](x, y, _ / _))

    // Math functions
    fr.registerFunction1[Double, Double]('sin, x => Result.succ(Math.sin(x)))
    fr.registerFunction1[Double, Double]('cos, x => Result.succ(Math.cos(x)))
    fr.registerFunction1[Double, Double]('tan, x => Result.succ(Math.tan(x)))
    fr.registerFunction1[Double, Double]('cot, x => Result.succ(1.0 / Math.tan(x)))
    fr.registerAlias('tg, 'tan)
    fr.registerAlias('ctg, 'cot)

    // Logical operations
    fr.registerFunction1[Boolean, Boolean]('not, x => Result.succ(!x))
    fr.registerFunction2[Boolean, Boolean, Boolean](
      'and,
      (x, y) => coupleResults[Boolean, Boolean, Boolean](x, y, _ && _)
    )
    fr.registerFunction2[Boolean, Boolean, Boolean](
      'or,
      (x, y) => coupleResults[Boolean, Boolean, Boolean](x, y, _ || _)
    )
    fr.registerFunction2[Boolean, Boolean, Boolean](
      'xor,
      (x, y) => coupleResults[Boolean, Boolean, Boolean](x, y, _ ^ _)
    )

    // Return the registry
    fr
  }
}
