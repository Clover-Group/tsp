package ru.itclover.tsp.dsl.v2
import shapeless.{HList, HNil}

import scala.collection.mutable
import scala.reflect.ClassTag

class FunctionRegistry {
  private val functions: mutable.Map[(Symbol, Class[_], Class[_]), Function[_, _]] =
    mutable.Map.empty

  def registerFunction[H <: HList, R](name: Symbol, function: Function[H, R])
                                     (implicit cth: ClassTag[H], ctr: ClassTag[R]): Unit =
    functions((name, cth.runtimeClass, ctr.runtimeClass)) = function
  def getFunction[H <: HList, R](name: Symbol)
                                (implicit cth: ClassTag[H], ctr: ClassTag[R]): Option[Function[H, R]] =
    functions.get((name, cth.runtimeClass, ctr.runtimeClass)).asInstanceOf[Option[Function[H, R]]]
  def unregisterFunction[H <: HList, R](name: Symbol)
                                       (implicit cth: ClassTag[H], ctr: ClassTag[R]): Unit =
    functions.remove((name, cth.runtimeClass, ctr.runtimeClass))
}


object FunctionRegistry {
  def createDefault: FunctionRegistry = {
    val fr = new FunctionRegistry
    //fr.registerFunction('add, (x: Double :: Double :: HNil) => x(0) + x(1))
    //fr.registerFunction('sub, (x: Double :: Double :: HNil) => x(0) - x(1))
    //fr.registerFunction('mul, (x: Double :: Double :: HNil) => x(0) * x(1))
    //fr.registerFunction('div, (x: Double :: Double :: HNil) => x(0) / x(1))
    fr
  }
}