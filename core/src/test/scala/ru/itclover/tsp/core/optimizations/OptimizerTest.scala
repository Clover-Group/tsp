package ru.itclover.tsp.core.optimizations

import org.scalatest.{FlatSpec, FunSuite, Matchers}
import ru.itclover.tsp.core.Common._
import ru.itclover.tsp.core.{Event, Patterns}

class OptimizerTest extends FlatSpec with Matchers {

  it should "optimize simple patterns" in {

    val patterns = new Patterns[EInt] {}
    import patterns._

    import cats.instances.int._

    val pat = const(3).plus(const(2))

    new Optimizer[EInt].optimize(pat) should be(const(5))

  }

}
