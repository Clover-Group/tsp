package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.v2.{Fail, Result, Succ}
import shapeless.{HList, HNil}

import scala.collection.mutable
import scala.reflect.ClassTag

/*class FunctionRegistry {
  private val functions: mutable.Map[(Symbol, Seq[ASTType]), Seq[Any] => Any] =
    mutable.Map.empty
  private val reducers: mutable.Map[(Symbol, ASTType), ((Any, Any) => Any, Any)] =
    mutable.Map.empty

  def registerFunction(name: Symbol, argTypes: Seq[ASTType], function: Seq[Any] => Any): Unit =
    functions((name, argTypes)) = function

  def getFunction(name: Symbol, argTypes: Seq[ASTType]): Option[Seq[Any] => Any] =
    functions.get((name, argTypes))

  def unregisterFunction(name: Symbol, argTypes: Seq[ASTType]): Unit =
    functions.remove((name, argTypes))

  def registerReducer(
    name: Symbol,
    argType: ASTType,
    function: (Any, Any) => Any,
    initial: Any
  ): Unit =
    reducers((name, argType)) = (function, initial)

  def getReducer(name: Symbol, argType: ASTType): Option[((Any, Any) => Any, Any)] =
    reducers.get((name, argType))

  def unregisterReducer(name: Symbol, argType: ASTType): Unit =
    reducers.remove((name, argType))

}

object FunctionRegistry {

  def createDefault: FunctionRegistry = {
    val fr = new FunctionRegistry
    fr.registerFunction(
      'add,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] + x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'sub,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] - x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'mul,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] * x(1).asInstanceOf[Double]
    )
    fr.registerFunction(
      'div,
      Seq(DoubleASTType, DoubleASTType),
      (x: Seq[Any]) => x(0).asInstanceOf[Double] / x(1).asInstanceOf[Double]
    )
    fr
  }
}*/

trait PFunction extends (Seq[Any] => Any)
trait PReducer extends ((Any, Any) => Any)
trait PReducerTransformation extends (Any => Any)

/**
  * Registry for runtime functions
  * Ensure that the result type of the function matches the corresponding ASTType. It's not automatic
  * @param functions Multi-argument functions (arguments wrapped into a Seq) and their return types
  * @param reducers Reducer functions, their return types and initial values
  */
case class FunctionRegistry(
  @transient functions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)],
  @transient reducers: Map[(Symbol, ASTType), (PReducer, ASTType, PReducerTransformation, Any)]
) {

  def ++(other: FunctionRegistry) = FunctionRegistry(functions ++ other.functions, reducers ++ other.reducers)
}

object DefaultFunctions {

  def arithmeticFunctions[T1: ClassTag, T2: ClassTag](
    implicit f: Fractional[T1],
    conv: T2 => T1
  ): Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    Map(
      ('add, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) => f.plus(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2]),
          astType1
        )
      ),
      ('sub, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) => f.minus(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2]),
          astType1
        )
      ),
      ('mul, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) => f.times(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2]),
          astType1
        )
      ),
      ('div, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) => f.div(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2]),
          astType1
        )
      ),
      ('add, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) => f.plus(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1]),
          astType1
        )
      ),
      ('sub, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) => f.minus(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1]),
          astType1
        )
      ),
      ('mul, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) => f.times(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1]),
          astType1
        )
      ),
      ('div, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) => f.div(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1]),
          astType1
        )
      )
    )
  }

  def mathFunctions[T: ClassTag](implicit conv: T => Double): Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType = ASTType.of[T]
    Map(
      ('abs, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => Math.abs(xs.head.asInstanceOf[T]),
          astType
        )
        ),
    )
  }

  val logicalFunctions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = Map(
    ('and, Seq(BooleanASTType, BooleanASTType)) -> (
      (
        (xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] && xs(1).asInstanceOf[Boolean],
        BooleanASTType
      )
    ),
    ('or, Seq(BooleanASTType, BooleanASTType)) -> (
      (
        (xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] || xs(1).asInstanceOf[Boolean],
        BooleanASTType
      )
    ),
    ('xor, Seq(BooleanASTType, BooleanASTType)) -> (
      (
        (xs: Seq[Any]) => xs(0).asInstanceOf[Boolean] ^ xs(1).asInstanceOf[Boolean],
        BooleanASTType
      )
    ),
    ('not, Seq(BooleanASTType)) -> (
      (
        (xs: Seq[Any]) => !xs(0).asInstanceOf[Boolean],
        BooleanASTType
      )
    )
  )

  def comparingFunctions[T1: ClassTag, T2: ClassTag](
    implicit ord: Ordering[T1],
    conv: T2 => T1
  ): Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    Map(
      ('lt, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.lt(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2])
          },
          BooleanASTType
        )
      ),
      ('le, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.lteq(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2])
          },
          BooleanASTType
        )
      ),
      ('gt, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.gt(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2])
          },
          BooleanASTType
        )
      ),
      ('ge, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.gteq(xs(0).asInstanceOf[T1], xs(1).asInstanceOf[T2])
          },
          BooleanASTType
        )
      ),
      ('eq, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            xs(0).asInstanceOf[T1] == xs(1).asInstanceOf[T2]
          },
          BooleanASTType
        )
      ),
      ('ne, Seq(astType1, astType2)) -> (
        (
          { (xs: Seq[Any]) =>
            xs(0).asInstanceOf[T1] != xs(1).asInstanceOf[T2]
          },
          BooleanASTType
        )
      ),
      ('lt, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.lt(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1])
          },
          BooleanASTType
        )
      ),
      ('le, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.lteq(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1])
          },
          BooleanASTType
        )
      ),
      ('gt, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.gt(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1])
          },
          BooleanASTType
        )
      ),
      ('ge, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            ord.gteq(xs(0).asInstanceOf[T2], xs(1).asInstanceOf[T1])
          },
          BooleanASTType
        )
      ),
      ('eq, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            xs(0).asInstanceOf[T2] == xs(1).asInstanceOf[T1]
          },
          BooleanASTType
        )
      ),
      ('ne, Seq(astType2, astType1)) -> (
        (
          { (xs: Seq[Any]) =>
            xs(0).asInstanceOf[T2] != xs(1).asInstanceOf[T1]
          },
          BooleanASTType
        )
      )
    )
  }

//  def lags[T: ClassTag]: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
//    val astType = ASTType.of[T]
//  }

  val reducers: Map[(Symbol, ASTType), (PReducer, ASTType, PReducerTransformation, Any)] = Map(
    ('sumof, DoubleASTType) -> (
      (
        (acc: Any, x: Any) => acc.asInstanceOf[Double] + x.asInstanceOf[Double],
        DoubleASTType,
        identity(_),
        0
      )
    ),
    ('minof, DoubleASTType) -> (
      (
        (acc: Any, x: Any) => Math.min(acc.asInstanceOf[Double], x.asInstanceOf[Double]),
        DoubleASTType,
        identity(_),
        0
      )
    ),
    ('maxof, DoubleASTType) -> (
      (
        (acc: Any, x: Any) => Math.max(acc.asInstanceOf[Double], x.asInstanceOf[Double]),
        DoubleASTType,
        identity(_),
        0
      )
    ),
    ('countof, DoubleASTType) -> ((acc: Any, x: Any) => acc.asInstanceOf[Double] + 1, DoubleASTType, identity(_), 0),
    ('avgof, DoubleASTType) -> (((acc: Any, x: Any) => {
      val (sum, count) = acc.asInstanceOf[(Double, Double)]
      (sum + x.asInstanceOf[Double], count + 1)
    }, DoubleASTType, (x: Any) => {
      val (sum, count) = x.asInstanceOf[(Double, Double)]
      sum / count
    }, 0))
  )

  // Fractional type for Int and Long to allow division

  implicit val fractionalInt: Fractional[Int] = new Fractional[Int] {
    override def div(x: Int, y: Int): Int = x / y
    override def plus(x: Int, y: Int): Int = x + y
    override def minus(x: Int, y: Int): Int = x - y
    override def times(x: Int, y: Int): Int = x * y
    override def negate(x: Int): Int = -x
    override def fromInt(x: Int): Int = x
    override def toInt(x: Int): Int = x
    override def toLong(x: Int): Long = x
    override def toFloat(x: Int): Float = x.toFloat
    override def toDouble(x: Int): Double = x.toDouble
    override def compare(x: Int, y: Int): Int = java.lang.Long.compare(x, y)
  }

  implicit val fractionalLong: Fractional[Long] = new Fractional[Long] {
    override def div(x: Long, y: Long): Long = x / y
    override def plus(x: Long, y: Long): Long = x + y
    override def minus(x: Long, y: Long): Long = x - y
    override def times(x: Long, y: Long): Long = x * y
    override def negate(x: Long): Long = -x
    override def fromInt(x: Int): Long = x
    override def toInt(x: Long): Int = x.toInt
    override def toLong(x: Long): Long = x
    override def toFloat(x: Long): Float = x.toFloat
    override def toDouble(x: Long): Double = x.toDouble
    override def compare(x: Long, y: Long): Int = java.lang.Long.compare(x, y)
  }
}

import DefaultFunctions._

object DefaultFunctionRegistry
    extends FunctionRegistry(
      functions = arithmeticFunctions[Int, Int] ++
      arithmeticFunctions[Long, Long] ++
      arithmeticFunctions[Double, Double] ++
      arithmeticFunctions[Double, Long] ++
      arithmeticFunctions[Double, Int] ++
      mathFunctions[Int] ++
      mathFunctions[Long] ++
      mathFunctions[Double] ++
      logicalFunctions ++
      comparingFunctions[Int, Int] ++
      comparingFunctions[Long, Long] ++
      comparingFunctions[Double, Double] ++
      comparingFunctions[Double, Long] ++
      comparingFunctions[Double, Int],
      reducers = reducers
    )
