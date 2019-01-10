package ru.itclover.tsp.dsl.v2
import shapeless.HList

class FunctionRegistry {
  def registerFunction[H <: HList, R](name: Symbol, function: Function[H, R]): Unit = ???
  def getFunction[H <: HList, R](name: Symbol): Option[Function[H, R]] = ???
  def unregisterFunction[H <: HList, R](name: Symbol): Unit = ???
}
