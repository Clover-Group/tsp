package ru.itclover.tsp.core.optimizations

import org.scalatest.{FlatSpec, FunSuite, Matchers}
import ru.itclover.tsp.core.Common._
import ru.itclover.tsp.core.{Event, IdxValue, PState, Pattern, Patterns, Result, SimplePattern, StateMachine, Window}

import scala.collection.mutable
import cats.Id

class OptimizerFootprint extends FlatSpec with Matchers {

  val patterns: Patterns[EInt] = new Patterns[EInt] {}
  import patterns._

  import cats.instances.int._

  def process[T, S <: PState[T, S]](pattern: Pattern[EInt, S, T], events: Seq[EInt]): Long = {
    val start = System.nanoTime()
    val sm = StateMachine[Id]
    val initialState = pattern.initialState()
    val collect = new mutable.ArrayBuffer[Long](events.size)
    val actState = sm.run(pattern, events, initialState, (x: IdxValue[T]) => collect += x.index, 1000)
    (System.nanoTime() - start) / 1000000

//    println(org.openjdk.jol.info.GraphLayout.parseInstance(actState).toFootprint)
  }

  def repeat[T, S <: PState[T, S]](times: Int, amount: Int, pattern: Pattern[EInt, S, T]): Long = {
    val events = (1 to amount).map(l => Event(l.toLong * 1000, 1, 1)).seq
    val ts = (1 to times).map(_ => { val t = process(pattern, events); println(t); t }).sum
    ts / times
  }

  // it should "run footprint benchmarks" in {

  //  val patternString = timer(field((e: EInt) => e.row) > const(0), Window(720 * 1000))
  //  println("String pattern: " + repeat(5, 1000000, patternString))

  //  val patternCouple = timer(patterns.assert(field(e => e.row).gt(const(0))), Window(720000))
  //  println("Couple pattern: " + repeat(5, 1000000, patternCouple))

  //  val patternMap = timer(patterns.assert(field(e => e.row).map(_ > 0)), Window(720000))
  //  println("Map pattern: " + repeat(5, 1000000, patternMap))

  //   val success = Result.succ(())
  //  val patternMap2 =
  //    timer(new SimplePattern((e: EInt) => if (e.row > 0) success else Result.fail), Window(720000))
  //  println("Map pattern: " + repeat(5, 1000000, patternMap2))

  //   val patternMap3 = timer(new SimplePattern((e: EInt) => if (e.row > 0) success else Result.fail), Window(720000))
  //   println("Map pattern3: " + repeat(10, 1000000, patternMap3))

  //  val optimizedPattern = new Optimizer[EInt].optimize(patternMap)
  //  println("Optimized pattern: " + repeat(5, 1000000, optimizedPattern))
  // }

}
