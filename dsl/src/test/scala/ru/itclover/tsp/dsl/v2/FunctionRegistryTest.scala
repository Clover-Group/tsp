package ru.itclover.tsp.dsl.v2
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.Result

class FunctionRegistryTest extends FlatSpec with Matchers with PropertyChecks {
  val funReg = DefaultFunctionRegistry
  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-6)

  "Function registry boolean functions" should "be callable" in {
    funReg.functions(('and, Seq(BooleanASTType, BooleanASTType)))._1(Seq(true, false)) shouldBe Result.succ(false)
    funReg.functions(('or, Seq(BooleanASTType, BooleanASTType)))._1(Seq(false, true)) shouldBe Result.succ(true)
    funReg.functions(('xor, Seq(BooleanASTType, BooleanASTType)))._1(Seq(true, true)) shouldBe Result.succ(false)
    funReg.functions(('not, Seq(BooleanASTType)))._1(Seq(true)) shouldBe Result.succ(false)
  }

//  "Function registry arithmetic functions with double" should "be callable" in {
//    funReg.functions(('add, Seq(DoubleASTType, DoubleASTType)))._1(Seq(5.0, 8.0)) shouldBe 13.0
//    funReg.functions(('sub, Seq(DoubleASTType, DoubleASTType)))._1(Seq(29.0, 16.0)) shouldBe 13.0
//    funReg.functions(('mul, Seq(DoubleASTType, DoubleASTType)))._1(Seq(13.0, 1.0)) shouldBe 13.0
//    funReg.functions(('div, Seq(DoubleASTType, DoubleASTType)))._1(Seq(78.0, 6.0)) shouldBe 13.0
//  }
//
//  "Function registry arithmetic functions with long" should "be callable" in {
//    funReg.functions(('add, Seq(LongASTType, LongASTType)))._1(Seq(5L, 8L)) shouldBe 13L
//    funReg.functions(('sub, Seq(LongASTType, LongASTType)))._1(Seq(29L, 16L)) shouldBe 13L
//    funReg.functions(('mul, Seq(LongASTType, LongASTType)))._1(Seq(13L, 1L)) shouldBe 13L
//    funReg.functions(('div, Seq(LongASTType, LongASTType)))._1(Seq(78L, 6L)) shouldBe 13L
//  }
//
//  "Function registry arithmetic functions with int" should "be callable" in {
//    funReg.functions(('add, Seq(IntASTType, IntASTType)))._1(Seq(5, 8)) shouldBe 13
//    funReg.functions(('sub, Seq(IntASTType, IntASTType)))._1(Seq(29, 16)) shouldBe 13
//    funReg.functions(('mul, Seq(IntASTType, IntASTType)))._1(Seq(13, 1)) shouldBe 13
//    funReg.functions(('div, Seq(IntASTType, IntASTType)))._1(Seq(78, 6)) shouldBe 13
//  }
//
//  "Function registry arithmetic functions with mixed types" should "be callable" in {
//    funReg.functions(('add, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](5.0, 8)) shouldBe 13.0
//    funReg.functions(('sub, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](29.0, 16)) shouldBe 13.0
//    funReg.functions(('mul, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](13.0, 1)) shouldBe 13.0
//    funReg.functions(('div, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](78.0, 6)) shouldBe 13.0
//  }
//
//  "Math functions" should "be callable" in {
//    // We need `.asInstanceOf[Double]` here to enable tolerant comparison
//    funReg.functions(('abs, Seq(DoubleASTType)))._1(Seq(-1.0)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('sin, Seq(DoubleASTType)))._1(Seq(Math.PI / 2)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('cos, Seq(DoubleASTType)))._1(Seq(Math.PI / 2)).asInstanceOf[Double] should === (0.0)
//    funReg.functions(('tan, Seq(DoubleASTType)))._1(Seq(Math.PI / 4)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('tg, Seq(DoubleASTType)))._1(Seq(Math.PI / 4)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('cot, Seq(DoubleASTType)))._1(Seq(Math.PI / 4)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('ctg, Seq(DoubleASTType)))._1(Seq(Math.PI / 4)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('sind, Seq(DoubleASTType)))._1(Seq(30.0)).asInstanceOf[Double] should === (0.5)
//    funReg.functions(('cosd, Seq(DoubleASTType)))._1(Seq(60.0)).asInstanceOf[Double] should === (0.5)
//    funReg.functions(('tand, Seq(DoubleASTType)))._1(Seq(45.0)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('tgd, Seq(DoubleASTType)))._1(Seq(0.0)).asInstanceOf[Double] should === (0.0)
//    funReg.functions(('cotd, Seq(DoubleASTType)))._1(Seq(45.0)).asInstanceOf[Double] should === (1.0)
//    funReg.functions(('ctgd, Seq(DoubleASTType)))._1(Seq(90.0)).asInstanceOf[Double] should === (0.0)
//  }
//
//  "Function registry comparison functions with double" should "be callable" in {
//    funReg.functions(('lt, Seq(DoubleASTType, DoubleASTType)))._1(Seq(5.0, 8.0)) shouldBe true
//    funReg.functions(('le, Seq(DoubleASTType, DoubleASTType)))._1(Seq(29.0, 18.0)) shouldBe false
//    funReg.functions(('gt, Seq(DoubleASTType, DoubleASTType)))._1(Seq(13.0, 12.0)) shouldBe true
//    funReg.functions(('ge, Seq(DoubleASTType, DoubleASTType)))._1(Seq(5.0, 6.0)) shouldBe false
//    funReg.functions(('eq, Seq(DoubleASTType, DoubleASTType)))._1(Seq(4.0, 4.0)) shouldBe true
//    funReg.functions(('ne, Seq(DoubleASTType, DoubleASTType)))._1(Seq(21.0, 6.0)) shouldBe true
//  }
//
//  "Function registry comparison functions with mixed types" should "be callable" in {
//    funReg.functions(('lt, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](5.0, 8)) shouldBe true
//    funReg.functions(('le, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](29.0, 18)) shouldBe false
//    funReg.functions(('gt, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](13.0, 12)) shouldBe true
//    funReg.functions(('ge, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](5.0, 6)) shouldBe false
//    funReg.functions(('eq, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](4.0, 4)) shouldBe true
//    funReg.functions(('ne, Seq(DoubleASTType, IntASTType)))._1(Seq[Any](21.0, 6)) shouldBe true
//  }

  "Function registry comparison functions with string" should "be callable" in {
    funReg.functions(('lt, Seq(StringASTType, StringASTType)))._1(Seq[Any]("abc", "abd")) shouldBe Result.succ(true)
    funReg.functions(('le, Seq(StringASTType, StringASTType)))._1(Seq[Any]("abc", "aba")) shouldBe Result.succ(false)
    funReg.functions(('gt, Seq(StringASTType, StringASTType)))._1(Seq[Any]("ghi", "def")) shouldBe Result.succ(true)
    funReg.functions(('ge, Seq(StringASTType, StringASTType)))._1(Seq[Any]("ghi", "gkl")) shouldBe Result.succ(false)
    funReg.functions(('eq, Seq(StringASTType, StringASTType)))._1(Seq[Any]("mno", "mno")) shouldBe Result.succ(true)
    funReg.functions(('ne, Seq(StringASTType, StringASTType)))._1(Seq[Any]("pqr", "stu")) shouldBe Result.succ(true)
  }
//
//  "Function registry concatenation" should "work" in {
//    // currently no other registries exist
//    funReg ++ funReg shouldBe funReg
//  }
//
//  "Function registry reducers" should "be callable" in {
//    funReg.reducers(('sumof, DoubleASTType))._1(5.0, 8.0) shouldBe 13.0
//    funReg.reducers(('minof, DoubleASTType))._1(5.0, 8.0) shouldBe 5.0
//    funReg.reducers(('maxof, DoubleASTType))._1(5.0, 8.0) shouldBe 8.0
//  }
}
