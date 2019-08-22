package ru.itclover.tsp.dsl.v2

import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.Result

class FunctionRegistryTest extends FlatSpec with Matchers with PropertyChecks {
  val funReg: DefaultFunctionRegistry.type = DefaultFunctionRegistry
  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-6)

  "Function registry boolean functions" should "be callable" in {

    val input = List(
      ('and, Seq(true, false)),
      ('or, Seq(false, true)),
      ('xor, Seq(true, true))
    )

    val output = List(false, true, false)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(BooleanASTType, BooleanASTType)))._1(in._2) shouldBe Result.succ(out)
    }
    funReg.functions(('not, Seq(BooleanASTType)))._1(Seq(true)) shouldBe Result.succ(false)
  }

  "Function registry arithmetic functions with double" should "be callable" in {

    val input = List(
      ('add, Seq(5.0, 8.0)),
      ('sub, Seq(29.0, 16.0)),
      ('mul, Seq(13.0, 1.0)),
      ('div, Seq(78.0, 6.0))
    )

    val output = List(13.0, 13.0, 13.0, 13.0)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(DoubleASTType, DoubleASTType)))._1(in._2) shouldBe Result.succ(out)
    }
  }

  "Function registry arithmetic functions with long" should "be callable" in {

    val input = List(
      ('add, Seq(5L, 8L)),
      ('sub, Seq(29L, 16L)),
      ('mul, Seq(13L, 1L)),
      ('div, Seq(78L, 6L))
    )

    val output = List(13L, 13L, 13L, 13L)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(LongASTType, LongASTType)))._1(in._2) shouldBe Result.succ(out)
    }

  }

  "Function registry arithmetic functions with int" should "be callable" in {

    val input = List(
      ('add, Seq(5, 8)),
      ('sub, Seq(29, 16)),
      ('mul, Seq(13, 1)),
      ('div, Seq(78, 6))
    )

    val output = List(13, 13, 13, 13)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(IntASTType, IntASTType)))._1(in._2) shouldBe Result.succ(out)
    }
  }

  "Function registry arithmetic functions with mixed types" should "be callable" in {

    val input = List(
      ('add, Seq(5.0, 8)),
      ('sub, Seq(29.0, 16)),
      ('mul, Seq(13.0, 1)),
      ('div, Seq(78.0, 6))
    )

    val output = List(13.0, 13.0, 13.0, 13.0)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](in._2: _*)) shouldBe Result.succ(out)
    }
  }

  "Math functions" should "be callable" in {

    val input = List(
      ('abs, Seq(-1.0), -1),
      ('sin, Seq(Math.PI / 2), Math.PI / 2),
      ('cos, Seq(Math.PI / 2), Math.PI / 2),
      ('tan, Seq(Math.PI / 4), Math.PI / 4),
      ('tg, Seq(Math.PI / 4), Math.PI / 4),
      ('cot, Seq(Math.PI / 4), Math.PI / 4),
      ('ctg, Seq(Math.PI / 4), Math.PI / 4),
      ('sind, Seq(30.0), 30.0),
      ('cosd, Seq(60.0), 60.0),
      ('tand, Seq(45.0), 45.0),
      ('tgd, Seq(0.0), 0.0),
      ('cotd, Seq(45.0), 45.0),
      ('ctgd, Seq(90.0), 90.0)
    )

    val output = List(
      1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, -0.01724440495424737, -0.01662274234697675, 0.028270410217108907,
      0.0, -28.716804167892423
    )

    (input, output).zipped.map { (in, out) =>
      // We need `.asInstanceOf[Double]` here to enable tolerant comparison
      funReg.functions((in._1, Seq(DoubleASTType)))._1(in._2).getOrElse(in._3).asInstanceOf[Double] should ===(out)
    }
  }

  "Function registry comparison functions with double" should "be callable" in {

    val input = List(
      ('lt, Seq(5.0, 8.0)),
      ('le, Seq(29.0, 18.0)),
      ('gt, Seq(13.0, 12.0)),
      ('ge, Seq(5.0, 6.0)),
      ('eq, Seq(4.0, 4.0)),
      ('ne, Seq(21.0, 6.0))
    )

    val output = List(true, false, true, false, true, true)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(DoubleASTType, DoubleASTType)))._1(in._2) shouldBe Result.succ(out)
    }
  }

  "Function registry comparison functions with mixed types" should "be callable" in {

    val input = List(
      ('lt, Seq(5.0, 8)),
      ('le, Seq(29.0, 18)),
      ('gt, Seq(13.0, 12)),
      ('ge, Seq(5.0, 6)),
      ('eq, Seq(4.0, 4)),
      ('ne, Seq(21.0, 6))
    )

    val output = List(true, false, true, false, true, true)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](in._2: _*)) shouldBe Result.succ(out)
    }
  }

  "Function registry comparison functions with string" should "be callable" in {

    val input = List(
      ('lt, Seq("abc", "abd")),
      ('le, Seq("abc", "aba")),
      ('gt, Seq("ghi", "def")),
      ('ge, Seq("ghi", "gkl")),
      ('eq, Seq("mno", "mno")),
      ('ne, Seq("pqr", "stu"))
    )

    val output = List(true, false, true, false, true, true)

    (input, output).zipped.map { (in, out) =>
      funReg.functions((in._1, Seq(StringASTType, StringASTType)))._1(Seq[Any](in._2: _*)) shouldBe Result.succ(out)
    }
  }

  "Function registry concatenation" should "work" in {
    // currently no other registries exist
    funReg ++ funReg shouldBe funReg
  }

//  "Function registry reducers" should "be callable" in {
//    funReg.reducers(('sumof, DoubleASTType))._1(5.0, 8.0) shouldBe 13.0
//    funReg.reducers(('minof, DoubleASTType))._1(5.0, 8.0) shouldBe 5.0
//    funReg.reducers(('maxof, DoubleASTType))._1(5.0, 8.0) shouldBe 8.0
//  }
}
