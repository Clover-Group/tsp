package ru.itclover.tsp.core.optimizations

import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.{IdxValue, PState, Pattern, Patterns, Result, SimplePattern, StateMachine, Window}

import scala.collection.mutable
import cats.Id
import ru.itclover.tsp.core.fixtures.Event

class OptimizerFootprint extends FlatSpec with Matchers {

  val patterns: Patterns[EInt] = new Patterns[EInt] {}
  import patterns._

  def process[T, S <: PState[T, S]](pattern: Pattern[EInt, S, T], events: Seq[EInt]): Long = {
    val start = System.nanoTime()
    val sm = StateMachine[Id]
    val initialState = pattern.initialState()
    val collect = new mutable.ArrayBuffer[Long](events.size)
    val actState = sm.run(pattern, events, initialState, (x: IdxValue[T]) => collect += x.index, 1000)
    (System.nanoTime() - start) / 1000000
  }

  def repeat[T, S <: PState[T, S]](times: Int, amount: Int, pattern: Pattern[EInt, S, T]): Long = {
    val events = (1 to amount).map(l => Event(l.toLong * 1000, 1, 1)).seq
    val ts = (1 to times).map(_ => { val t = process(pattern, events); t }).sum
    ts / times
  }

  it should "run footprint benchmarks" in {

    val expectedTimeForStringAndCouple = 1500
    val expectedTimeForMapAndOptimize = 1000

    val patternString = timer(field((e: EInt) => e.row) > const(0), Window(720 * 1000))
    val actualStringTime = repeat(5, 1000000, patternString)
    assert(actualStringTime > expectedTimeForStringAndCouple)

    val patternCouple = timer(patterns.assert(field(e => e.row).gt(const(0))), Window(720000))
    val actualCoupleTime = repeat(5, 1000000, patternCouple)
    assert(actualCoupleTime > expectedTimeForStringAndCouple)

    val patternMapFirst = timer(patterns.assert(field(e => e.row).map(_ > 0)), Window(720000))
    val actualMapFirstTime = repeat(5, 1000000, patternMapFirst)
    assert(actualMapFirstTime > expectedTimeForMapAndOptimize)

    val success = Result.succ(())
    val patternMapSecond = timer(SimplePattern((e: EInt) => if (e.row > 0) success else Result.fail), Window(720000))
    val actualMapSecondTime = repeat(5, 1000000, patternMapSecond)
    assert(actualMapSecondTime > expectedTimeForMapAndOptimize)

    val patternMapThird = timer(SimplePattern((e: EInt) => if (e.row > 0) success else Result.fail), Window(720000))
    val actualMapThirdTime = repeat(10, 1000000, patternMapThird)
    assert(actualMapThirdTime > expectedTimeForMapAndOptimize)

    val optimizedPattern = new Optimizer[EInt].optimize(patternMapFirst)
    val actualOptimizeTime = repeat(5, 1000000, optimizedPattern)
    assert(actualOptimizeTime > expectedTimeForMapAndOptimize)
  }

}
