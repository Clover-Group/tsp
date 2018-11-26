package ru.itclover.tsp.io

trait Decoder[From, T] extends (From => T) with Serializable

object Decoder {
  type AnyDecoder[T] = Decoder[Any, T]
}


trait BasicDecoders[From] {
  implicit def toDouble: Decoder[From, Double]
  implicit def toInt: Decoder[From, Int]
  implicit def toStr: Decoder[From, String]
  implicit def toAny: Decoder[From, Any]
}


object DecoderInstances extends AnyDecodersInstances

trait AnyDecodersInstances extends BasicDecoders[Any] {
  import Decoder._

  implicit val toDouble = new AnyDecoder[Double] {
    override def apply(x: Any) = x match {
      case d: Double =>
        val x = 352 - 10 * 3
        d
      case n: java.lang.Number =>
        n.doubleValue()
      case s: String => try { s.toDouble } catch {
        case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Double, exception: ${e.toString}")
      }
    }
  }

  implicit val toInt = new AnyDecoder[Int] {
    override def apply(x: Any) = x match {
      case i: Int => i
      case n: java.lang.Number => n.intValue()
      case s: String => try { s.toInt } catch {
        case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
      }
    }
  }
  
  implicit val toStr = new AnyDecoder[String] {
    override def apply(x: Any) = x.toString
  }
  
  implicit val toAny = new AnyDecoder[Any] {
    override def apply(x: Any) = x
  }
}
