package ru.itclover.tsp.dsl

import java.io.Serializable
import com.typesafe.scalalogging.LazyLogging
import ru.itclover.tsp.core.{Fail, Result, Succ}

import scala.reflect.ClassTag
import scala.util.Try

type PFunction = (Seq[Result[Any]] => Result[Any]) 

type PReducer = ((Result[Any], Any) => Result[Any])

type PReducerTransformation = (Result[Any] => Result[Any])

/**
  * Registry for runtime functions
  * Ensure that the result type of the function matches the corresponding ASTType. It's not automatic
  *
  * @param functions Multi-argument functions (arguments wrapped into a Seq) and their return types
  * @param reducers Reducer functions, their return types and initial values
  */
@SuppressWarnings(Array("org.wartremover.warts.Serializable"))
class FunctionRegistry(
  @transient val functions: Map[(String, Seq[ASTType]), (PFunction, ASTType)],
  @transient val reducers: Map[(String, ASTType), (PReducer, ASTType, PReducerTransformation, Serializable)]
) {

  def ++(other: FunctionRegistry) = FunctionRegistry(functions ++ other.functions, reducers ++ other.reducers)

  def findBestFunctionMatch(name: String, types: Seq[ASTType]): Option[((PFunction, ASTType), Long)] =
    functions
      .filterKeys {
        case (n, t) => n == name && t.length == types.length
      }
      .toList
      .map {
        case x @ ((_, t), _) => (x, t.zip(types).map { case (to, from) => FunctionRegistry.castability(from, to) }.sum)
      }
      .sortBy(-_._2)
      .find(_._2 > 0)
      .map { case ((_, f), c) => (f, c) }
}

object FunctionRegistry {

  /**
    * How good types can be cast.
    * @param from Source type
    * @param to Destination type
    * @return Measure of castability (1 = worst possible, 9 = best possible, 10 = same types)
    */
  def castability(from: ASTType, to: ASTType): Long = (from, to) match {
    case (x, y) if x == y                => 10 // Same types
    case (NullASTType, _)                => 9 // Null can be cast to any type (with highest priority)
    case (IntASTType, DoubleASTType)     => 9 // Int can be safely cast to double
    case (IntASTType, LongASTType)       => 9 // Int can be safely cast to Long
    case (BooleanASTType, IntASTType)    => 9 // Boolean can be safely cast to Int
    case (BooleanASTType, LongASTType)   => 9 // Boolean can be safely cast to Long
    case (BooleanASTType, DoubleASTType) => 8 // Boolean can be safely cast to Double, but integral type is preferred
    case (DoubleASTType, LongASTType)    => 4 // Possible precision loss (but Long is preferred anyway)
    case (DoubleASTType, IntASTType)     => 3 // Possible precision loss
    case (IntASTType, BooleanASTType)    => 7 // Integral types can be cast to Boolean (zero/nonzero), but with caution
    case (LongASTType, BooleanASTType)   => 7 // Integral types can be cast to Boolean (zero/nonzero), but with caution
    case (DoubleASTType, BooleanASTType) => 2 // Use very cautiously (value even very close to zero is still TRUE)
    case (_, StringASTType)              => 1 // Any type can be cast to string, but with lowest priority
    case _                               => Int.MinValue // no casting otherwise (not Long.MinValue since we use addition)
  }
}

// Function registry uses Any
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object DefaultFunctions extends LazyLogging {

  // Here, asInstanceOf is used in a safe way (and conversion from null).
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def toResult[T](x: Any)(implicit ct: ClassTag[T]): Result[T] =
    x match {
      case value: Result[_]                                          => value.asInstanceOf[Result[T]]
      case value: T                                                  => Result.succ(value)
      case null                                                      => logger.warn(s"Null value arrived with type $ct"); Result.fail //fromNull[T]
      case value if ct.runtimeClass.isAssignableFrom(value.getClass) => Result.succ(value.asInstanceOf[T])
      case v: Long if (ct.runtimeClass eq classOf[Int]) || (ct.runtimeClass eq classOf[java.lang.Integer]) =>
        Result.succ(v.toInt.asInstanceOf[T]) // we know that T == Int
      case v: Long if (ct.runtimeClass eq classOf[Double]) || (ct.runtimeClass eq classOf[java.lang.Double]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Double
      case v: Int if (ct.runtimeClass eq classOf[Long]) || (ct.runtimeClass eq classOf[java.lang.Long]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Long
      case v: Int if (ct.runtimeClass eq classOf[Double]) || (ct.runtimeClass eq classOf[java.lang.Double]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Double
      case v: java.lang.Long if (ct.runtimeClass eq classOf[Int]) || (ct.runtimeClass eq classOf[java.lang.Integer]) =>
        Result.succ(v.toInt.asInstanceOf[T]) // we know that T == Int
      case v: java.lang.Long if (ct.runtimeClass eq classOf[Double]) || (ct.runtimeClass eq classOf[java.lang.Double]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Double
      case v: java.lang.Integer if (ct.runtimeClass eq classOf[Long]) || (ct.runtimeClass eq classOf[java.lang.Long]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Long
      case v: java.lang.Integer
          if (ct.runtimeClass eq classOf[Double]) || (ct.runtimeClass eq classOf[java.lang.Double]) =>
        Result.succ(v.toDouble.asInstanceOf[T]) // we know that T == Double
      case _ if ct.runtimeClass.isAssignableFrom(classOf[String]) =>
        Result.succ(x.toString.asInstanceOf[T]) // we know that T is assignable from String
      // TODO: maybe some other cases
      case _ =>
        logger.warn(s"$x (of type ${x.getClass.getName}) cannot be cast to $ct")
        Result.fail
    }

//  private def fromNull[T](implicit ct: ClassTag[T]): Result[T] = ct.runtimeClass match {
//    case x if (x eq classOf[Double]) || (x eq classOf[java.lang.Double]) => Result.succ(Double.NaN.asInstanceOf[T])
//    case x if (x eq classOf[String]) || (x eq classOf[java.lang.String]) => Result.succ("".asInstanceOf[T])
//    case _ => Result.fail
//  }

  def arithmeticFunctions[T1: ClassTag, T2: ClassTag](
    implicit f: Fractional[T1],
    conv: Conversion[T2, T1]
  ): Map[(String, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    def func(f: (T1, T2) => T1): (PFunction, ASTType) = (
      (xs: Seq[Any]) =>
        (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
          case (Succ(t0), Succ(t1)) => Result.succ(f(t0, t1))
          case _                    => Result.fail
        },
      astType1
    )
    Map(
      ("add", Seq(astType1, astType2)) -> func(f.plus(_, _)),
      ("sub", Seq(astType1, astType2)) -> func(f.minus(_, _)),
      ("mul", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.times(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ("div", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.div(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ("add", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.plus(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ("sub", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.minus(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ("mul", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(f.times(t0, t1))
              case _                    => Result.fail
            },
          astType1
        )
      ),
      ("div", Seq(astType2, astType1)) -> (
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

  def mathFunctions[T: ClassTag](implicit conv: Conversion[T, Double]): Map[(String, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType = ASTType.of[T]
    Map(
      ("abs", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.abs(_)),
          astType
        )
      ),
      ("sin", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.sin(_)),
          astType
        )
      ),
      ("cos", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.cos(_)),
          astType
        )
      ),
      ("tan", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.tan(_)),
          astType
        )
      ),
      ("tg", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(Math.tan(_)),
          astType
        )
      ),
      ("cot", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(1.0 / Math.tan(_)),
          astType
        )
      ),
      ("ctg", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(1.0 / Math.tan(_)),
          astType
        )
      ),
      ("sind", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.sin(Math.toRadians(x))),
          astType
        )
      ),
      ("cosd", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.cos(Math.toRadians(x))),
          astType
        )
      ),
      ("tand", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.tan(Math.toRadians(x))),
          astType
        )
      ),
      ("tgd", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => Math.tan(Math.toRadians(x))),
          astType
        )
      ),
      ("cotd", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => 1.0 / Math.tan(Math.toRadians(x))),
          astType
        )
      ),
      ("ctgd", Seq(astType)) -> (
        (
          (xs: Seq[Any]) => toResult[T](xs(0)).map(x => 1.0 / Math.tan(Math.toRadians(x))),
          astType
        )
      )
    )
  }

  def logicalFunctions: Map[(String, Seq[ASTType]), (PFunction, ASTType)] = {
    // TSP-182 - Workaround for correct type inference

    val btype = BooleanASTType

    def func(sym: String, xs: Seq[Any])(implicit l: Logical[Any]): Result[Boolean] = {

      //log.debug(s"func($sym): Arg0 = $xs.head, Arg1 = $xs(1)")
      //log.info(s"Args = ${(xs.head, xs.lift(1).getOrElse(Unit))}")
      //log.info(s"Arg results = ${(toResult[Boolean](xs.head), toResult[Boolean](xs.lift(1).getOrElse(Unit)))}")
      (toResult[Boolean](xs(0)), toResult[Boolean](xs.lift(1).getOrElse(()))) match {
        case (Succ(x0), Succ(x1)) =>
          sym match {

            case "and" => Result.succ(l.and(x0, x1))
            case "or"  => Result.succ(l.or(x0, x1))
            case "xor" => Result.succ(l.xor(x0, x1))
            case "eq"  => Result.succ(l.eq(x0, x1))
            case "neq" => Result.succ(l.neq(x0, x1))
            case _     => Result.fail
          }
        case (Succ(x0), Fail) =>
          sym match {
            case "not" => Result.succ(l.not(x0))
            case "or"  => Result.succ(x0)
            case _     => Result.fail
          }
        case (Fail, Succ(x1)) =>
          sym match {
            case "or" => Result.succ(x1)
            case _    => Result.fail
          }
        case _ => Result.fail
      }
    }

    Map(
      //('and , Seq(btype, btype))  -> (((xs: Seq[Any]) => xs.foldLeft(true) {_.asInstanceOf[Boolean] && _.asInstanceOf[Boolean]}, btype)),
      //('or  , Seq(btype, btype))  -> (((xs: Seq[Any]) => xs.foldLeft(true) {_.asInstanceOf[Boolean] || _.asInstanceOf[Boolean]}, btype)),
      ("and", Seq(btype, btype)) -> (((xs: Seq[Any]) => func("and", xs), btype)),
      ("or", Seq(btype, btype))  -> (((xs: Seq[Any]) => func("or", xs), btype)),
      ("xor", Seq(btype, btype)) -> (((xs: Seq[Any]) => func("xor", xs), btype)),
      ("eq", Seq(btype, btype))  -> (((xs: Seq[Any]) => func("eq", xs), btype)),
      ("neq", Seq(btype, btype)) -> (((xs: Seq[Any]) => func("neq", xs), btype)),
      ("not", Seq(btype))        -> (((xs: Seq[Any]) => func("not", xs), btype))
    )
  }

  def comparingFunctions[T1: ClassTag, T2: ClassTag](
    implicit ord: Ordering[T1],
    conv: Conversion[T2, T1]
  ): Map[(String, Seq[ASTType]), (PFunction, ASTType)] = {
    val astType1: ASTType = ASTType.of[T1]
    val astType2: ASTType = ASTType.of[T2]
    Map(
      ("lt", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lt(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("le", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lteq(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("gt", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gt(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("ge", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gteq(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("eq", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.equiv(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("ne", Seq(astType1, astType2)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T1](xs(0)), toResult[T2](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(!ord.equiv(t0, conv(t1)))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("lt", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lt(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("le", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.lteq(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("gt", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gt(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("ge", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.gteq(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("eq", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(ord.equiv(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      ),
      ("ne", Seq(astType2, astType1)) -> (
        (
          (xs: Seq[Any]) =>
            (toResult[T2](xs(0)), toResult[T1](xs(1))) match {
              case (Succ(t0), Succ(t1)) => Result.succ(!ord.equiv(conv(t0), t1))
              case _                    => Result.fail
            },
          BooleanASTType
        )
      )
    )
  }

  def reducers[T: ClassTag](
    implicit conv: Conversion[T, Double]
  ): Map[(String, ASTType), (PReducer, ASTType, PReducerTransformation, Serializable)] = Map(
    ("sumof", DoubleASTType) -> (
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
    ("minof", DoubleASTType) -> (
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
    ("maxof", DoubleASTType) -> (
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
    ("countof", DoubleASTType) -> (({ (acc: Result[Any], x: Any) =>
      (toResult[Double](acc), toResult[Double](x)) match {
        case (Succ(da), Succ(_)) => Result.succ(da + 1)
        case _                   => Result.fail
      }
    }, DoubleASTType, {
      identity(_)
    }, java.lang.Double.valueOf(0))),
    ("avgof", DoubleASTType) -> (({ (acc: Result[Any], x: Any) =>
      (toResult[(Double, Double)](acc), toResult[Double](x)) match {
        case (Succ((sum, count)), Succ(dx)) => Result.succ((sum + dx, count + 1))
        case _                              => Result.fail
      }
    }, DoubleASTType, {
      case Succ((sum: Double, count: Double)) => Result.succ(sum / count)
      case _                                  => Result.fail
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
    override def toLong(x: Int): Long = x.toLong
    override def toFloat(x: Int): Float = x.toFloat
    override def toDouble(x: Int): Double = x.toDouble
    override def compare(x: Int, y: Int): Int = java.lang.Long.compare(x.toLong, y.toLong)

    override def parseString(str: String): Option[Int] = Try(str.toInt).toOption
  }

  implicit val fractionalLong: Fractional[Long] = new Fractional[Long] {
    override def div(x: Long, y: Long): Long = x / y
    override def plus(x: Long, y: Long): Long = x + y
    override def minus(x: Long, y: Long): Long = x - y
    override def times(x: Long, y: Long): Long = x * y
    override def negate(x: Long): Long = -x
    override def fromInt(x: Int): Long = x.toLong
    override def toInt(x: Long): Int = x.toInt
    override def toLong(x: Long): Long = x
    override def toFloat(x: Long): Float = x.toFloat
    override def toDouble(x: Long): Double = x.toDouble
    override def compare(x: Long, y: Long): Int = java.lang.Long.compare(x, y)

    override def parseString(str: String): Option[Long] = Try(str.toLong).toOption

  }
}

import ru.itclover.tsp.dsl.DefaultFunctions._



object DefaultFunctionRegistry {
  given Conversion[Int, Int] = _.toInt
  given Conversion[Int, Long] = _.toLong
  given Conversion[Long, Long] = _.toLong
  given Conversion[Int, Double] = _.toDouble
  given Conversion[Long, Double] = _.toDouble
  given Conversion[Double, Double] = _.toDouble
  given Conversion[String, String] = _.toString

  given Ordering[Float] = scala.math.Ordering.Float.IeeeOrdering
  given Ordering[Double] = scala.math.Ordering.Double.IeeeOrdering

  val defaultFunctions = arithmeticFunctions[Int, Int] ++
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
        comparingFunctions[Double, Int] ++
        comparingFunctions[String, String]

  val defaultReducers = reducers[Int] ++ reducers[Long] ++ reducers[Double]

  val registry = FunctionRegistry(functions = defaultFunctions, reducers = defaultReducers)
}

