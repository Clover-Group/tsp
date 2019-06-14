package ru.itclover.tsp.core.optimizations

import org.scalatest.{FlatSpec, FunSuite, Matchers}
import ru.itclover.tsp.core.Common._
import ru.itclover.tsp.core.{Event, Patterns, SimplePattern}

class OptimizerTest extends FlatSpec with Matchers {

  val patterns: Patterns[EInt] = new Patterns[EInt] {}
  import patterns._

  import cats.instances.int._

  it should "optimize couple(const, const) to const" in {

    val pat = const(3).plus(const(2))

    new Optimizer[EInt].optimize(pat) should be(const(5))

  }

  it should "optimize map(const) to const" in {
    val pat = const(3).map(_ + 2)

    new Optimizer[EInt].optimize(pat) should be(const(5))
  }

  it should "optimize map(simple) to simple" in {

    val pat = field(_.col).map(_ + 2)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.SimplePattern[EInt, _]]
  }

  it should "optimize couple(simple, const) to simple" in {}

  it should "optimize couple(const, simple) to simple" in {}

  it should "optimize couple(simple, simple) to simple" in {}
}
