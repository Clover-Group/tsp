package ru.itclover.tsp.io

trait Decoder[From, T] extends (From => T) with Serializable

object Decoder {
  type AnyDecoder[T] = Decoder[Any, T]
}


trait BasicDecoders[From] {
  implicit def decodeToDouble: Decoder[From, Double]
  implicit def decodeToInt: Decoder[From, Int]
  implicit def decodeToString: Decoder[From, String]
  implicit def decodeToAny: Decoder[From, Any]
}


object DecoderInstances extends AnyDecodersInstances

trait AnyDecodersInstances extends BasicDecoders[Any] {
  import Decoder._

  implicit val decodeToDouble: Decoder[Any, Double] = new AnyDecoder[Double] {
    override def apply(x: Any) = x match {
      case d: Double => d
      case n: java.lang.Number => n.doubleValue()
      case s: String => try { java.lang.Double.parseDouble(s)} catch {
        case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Double, exception: ${e.toString}")
      }
    }
  }

  implicit val decodeToInt: Decoder[Any, Int] = new AnyDecoder[Int] {
    override def apply(x: Any) = x match {
      case i: Int => i
      case n: java.lang.Number => n.intValue()
      case s: String => 
        try { Helper.strToInt(s) } catch {
        case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
      }
    }
  }
  
  implicit val decodeToString: Decoder[Any, String] = new AnyDecoder[String] {
    override def apply(x: Any) = x.toString
  }
  
  implicit val decodeToAny: Decoder[Any, Any] = new AnyDecoder[Any] {
    override def apply(x: Any) = x
  }
}

// Hack for  String.toInt implicit method
object Helper {
  def strToInt(s: String) = s.toInt
}