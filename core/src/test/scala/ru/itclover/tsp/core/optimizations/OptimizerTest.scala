package ru.itclover.tsp.core.optimizations

import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.Common._
import ru.itclover.tsp.core.Patterns
import ru.itclover.tsp.core.Window

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

  it should "optimize couple(simple, const) to simple" in {
    val pat = field(_.col) > const(3)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.SimplePattern[EInt, _]]
  }

  it should "optimize couple(const, simple) to simple" in {
    val pat = const(3) > field(_.col)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.SimplePattern[EInt, _]]
  }

  it should "optimize couple(simple, simple) to simple" in {
    val pat = field(_.col) > field(_.col)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.SimplePattern[EInt, _]]
  }

  it should "optimize map(map(simple) to simple" in {
    val pat = field(_.col).map(_ + 2).map(_ * 3)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.SimplePattern[EInt, _]]
  }

  it should "optimize couple(some, const) to map" in {
    val pat = timer(field(_.col), Window(1000)).map(_ + 2) > const(3)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.MapPattern[EInt, _, _, _]]
  }

  it should "optimize couple(const, some) to map" in {
    val pat = const(3) > timer(field(_.col), Window(1000)).map(_ + 2)

    new Optimizer[EInt].optimize(pat) shouldBe a[ru.itclover.tsp.core.MapPattern[EInt, _, _, _]]
  }

}
