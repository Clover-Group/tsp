package ru.itclover.tsp.dsl.v2
import java.io.Serializable

import ru.itclover.tsp.core.{Fail, Result, Succ}

//import scala.collection.mutable
import scala.reflect.ClassTag
import com.typesafe.scalalogging.Logger

@SerialVersionUID(81001L)
trait PFunction extends (Seq[Any] => Result[Any]) with Serializable

@SerialVersionUID(81002L)
trait PReducer extends ((Result[Any], Any) => Result[Any]) with Serializable

@SerialVersionUID(81003L)
trait PReducerTransformation extends (Result[Any] => Result[Any]) with Serializable

/**
  * Registry for runtime functions
  * Ensure that the result type of the function matches the corresponding ASTType. It's not automatic
  *
  * @param functions Multi-argument functions (arguments wrapped into a Seq) and their return types
  * @param reducers Reducer functions, their return types and initial values
  */
case class FunctionRegistry(
  @transient functions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)],
  @transient reducers: Map[(Symbol, ASTType), (PReducer, ASTType, PReducerTransformation, Serializable)]
) {

  def ++(other: FunctionRegistry) = FunctionRegistry(functions ++ other.functions, reducers ++ other.reducers)
}

object DefaultFunctions {

  val log = Logger("DefaultFunctionRegistry")

  private def toResult[T](x: Any)(implicit ct: ClassTag[T]): Result[T] = {
    x match {
      case value: Result[T]                                          => value
      case value: T                                                  => Result.succ(value)
      case value if ct.runtimeClass.isAssignableFrom(value.getClass) => Result.succ(value.asInstanceOf[T])
      case v: Long if (ct.runtimeClass eq classOf[Int]) || (ct.runtimeClass eq classOf[java.lang.Integer]) =>
        Result.succ(v.toInt.asInstanceOf[T]) // we know that T == Int
      case v: Long if (ct.runtimeClass eq classOf[Double]) || (ct.runtimeClass eq classOf[java.lang.Double]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Double
      // TODO: maybe some other cases
      case _ =>
        log.warn(s"$x (of type ${x.getClass.getName}) cannot be cast to $ct")
        Result.fail
    }
  }

  def arithmeticFunctions[T1: ClassTag, T2: ClassTag](
    implicit f: Fractional[T1],
    conv: T2 => T1
  ): Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    def func(f: (T1, T2) => T1): (PFunction, ASTType) = (
      (xs: Seq[Any]) =>
        (toResult[T1](xs.head), toResult[T2](xs(1))) match {
          case (Succ(t0), Succ(t1)) => Result.succ(f(t0, t1))
          case _                    => Result.fail
        },
      astType1
    )
    Map(
      ('add, Seq(astType1, astType2)) -> func(f.plus(_, _)),
      ('sub, Seq(astType1, astType2)) -> func(f.minus(_, _)),
      ('mul, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.times(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ('div, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.div(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ('add, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.plus(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ('sub, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.minus(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ('mul, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.times(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ('div, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.div(t0, t1))
              case _                    => Result.fail
            },
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
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.abs(_)),
          astType
        )
      ),
      ('sin, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.sin(_)),
          astType
        )
      ),
      ('cos, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.cos(_)),
          astType
        )
      ),
      ('tan, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.tan(_)),
          astType
        )
      ),
      ('tg, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.tan(_)),
          astType
        )
      ),
      ('cot, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(1.0 / Math.tan(_)),
          astType
        )
      ),
      ('ctg, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(1.0 / Math.tan(_)),
          astType
        )
      ),
      ('sind, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.toRadians(Math.sin(x))),
          astType
        )
      ),
      ('cosd, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.toRadians(Math.cos(x))),
          astType
        )
      ),
      ('tand, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.toRadians(Math.tan(x))),
          astType
        )
      ),
      ('tgd, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.toRadians(Math.sin(x))),
          astType
        )
      ),
      ('cotd, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => 1.0 / Math.toRadians(Math.tan(x))),
          astType
        )
      ),
      ('ctgd, Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => 1.0 / Math.toRadians(Math.tan(x))),
          astType
        )
      )
    )
  }

  def logicalFunctions: Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    import Functional._
    val log = Logger("LogicalLogger")

    // TSP-182 - Workaround for correct type inference

    val btype = BooleanASTType

    def func(sym: Symbol, xs: Seq[Any])(implicit l: Logical[Any]): Result[Boolean] = {

      //log.debug(s"func($sym): Arg0 = $xs(0), Arg1 = $xs(1)")
      //log.info(s"Args = ${(xs(0), xs.lift(1).getOrElse(Unit))}")
      //log.info(s"Arg results = ${(toResult[Boolean](xs(0)), toResult[Boolean](xs.lift(1).getOrElse(Unit)))}")
      (toResult[Boolean](xs(0)), toResult[Boolean](xs.lift(1).getOrElse(Unit))) match {
        case (Succ(x0), Succ(x1)) =>
          sym match {

            case 'and => Result.succ(l.and(x0, x1))
            case 'or  => Result.succ(l.or(x0, x1))
            case 'xor => Result.succ(l.xor(x0, x1))
            case 'eq  => Result.succ(l.eq(x0, x1))
            case 'neq => Result.succ(l.neq(x0, x1))
            case _    => Result.fail
          }
        case (Succ(x0), Fail) =>
          sym match {
            case 'not => Result.succ(l.not(x0))
            case _    => Result.fail
          }
        case _ => Result.fail
      }
    }

    Map(
      //('and , Seq(btype, btype))  -> (((xs: Seq[Any]) => xs.foldLeft(true) {_.asInstanceOf[Boolean] && _.asInstanceOf[Boolean]}, btype)),
      //('or  , Seq(btype, btype))  -> (((xs: Seq[Any]) => xs.foldLeft(true) {_.asInstanceOf[Boolean] || _.asInstanceOf[Boolean]}, btype)),
      ('and, Seq(btype, btype)) -> (((xs: Seq[Any]) => func('and, xs), btype)),
      ('or, Seq(btype, btype))  -> (((xs: Seq[Any]) => func('or, xs), btype)),
      ('xor, Seq(btype, btype)) -> (((xs: Seq[Any]) => func('xor, xs), btype)),
      ('eq, Seq(btype, btype))  -> (((xs: Seq[Any]) => func('eq, xs), btype)),
      ('neq, Seq(btype, btype)) -> (((xs: Seq[Any]) => func('neq, xs), btype)),
      ('not, Seq(btype))        -> (((xs: Seq[Any]) => func('not, xs), btype))
    )
  }

  def comparingFunctions[T1: ClassTag, T2: ClassTag](
    implicit ord: Ordering[T1],
    conv: T2 => T1
  ): Map[(Symbol, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    Map(
      ('lt, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lt(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('le, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lteq(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('gt, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gt(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('ge, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gteq(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('eq, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.equiv(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('ne, Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(!ord.equiv(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('lt, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lt(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('le, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lteq(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('gt, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gt(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('ge, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gteq(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('eq, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.equiv(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ('ne, Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(!ord.equiv(t0, t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      )
    )
  }

  def reducers[T: ClassTag](
    implicit conv: T => Double
  ): Map[(Symbol, ASTType), (PReducer, ASTType, PReducerTransformation, Serializable)] = Map(
    ('sumof, DoubleASTType) -> (
      (
        { (acc: Result[Any], x: Any) =>
          (toResult[Double](acc), toResult[Double](x)) match {
            case (Succ(da), Succ(dx)) => Result.succ(da + dx)
            case _                    => Result.fail
          }
        },
        DoubleASTType, {
          identity(_)
        },
        java.lang.Double.valueOf(0)
      )
    ),
    ('minof, DoubleASTType) -> (
      (
        { (acc: Result[Any], x: Any) =>
          (toResult[Double](acc), toResult[Double](x)) match {
            case (Succ(da), Succ(dx)) => Result.succ(Math.min(da, dx))
            case _                    => Result.fail
          }
        },
        DoubleASTType, {
          identity(_)
        },
        java.lang.Double.valueOf(Double.MaxValue)
      )
    ),
    ('maxof, DoubleASTType) -> (
      (
        { (acc: Result[Any], x: Any) =>
          (toResult[Double](acc), toResult[Double](x)) match {
            case (Succ(da), Succ(dx)) => Result.succ(Math.max(da, dx))
            case _                    => Result.fail
          }
        },
        DoubleASTType, {
          identity(_)
        },
        java.lang.Double.valueOf(Double.MinValue)
      )
    ),
    ('countof, DoubleASTType) -> ({ (acc: Result[Any], x: Any) =>
      (toResult[Double](acc), toResult[Double](x)) match {
        case (Succ(da), Succ(_)) => Result.succ(da + 1)
        case _                   => Result.fail
      }
    }, DoubleASTType, {
      identity(_)
    }, java.lang.Double.valueOf(0)),
    ('avgof, DoubleASTType) -> (({ (acc: Result[Any], x: Any) =>
      (toResult[(Double, Double)](acc), toResult[Double](x)) match {
        case (Succ((sum, count)), Succ(dx)) => Result.succ((sum + dx, count + 1))
        case _                              => Result.fail
      }
    }, DoubleASTType, { x: Result[Any] =>
      x match {
        case Succ((sum: Double, count: Double)) => Result.succ(sum / count)
        case _                                  => Result.fail
      }
    }, (0.0, 0.0)))
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
      arithmeticFunctions[Long, Int] ++
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
      reducers = reducers[Int] ++ reducers[Long] ++ reducers[Double]
    )
