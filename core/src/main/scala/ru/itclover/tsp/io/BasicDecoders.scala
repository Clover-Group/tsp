package ru.itclover.tsp.io

trait BasicDecoders[From] {
  implicit def decodeToDouble: Decoder[From, Double]
  implicit def decodeToInt: Decoder[From, Int]
  implicit def decodeToString: Decoder[From, String]
  implicit def decodeToAny: Decoder[From, Any]
}

object AnyDecodersInstances extends BasicDecoders[Any] {
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

  implicit val decodeToString: Decoder[Any, String] = Decoder { x: Any => x.toString }

  implicit val decodeToAny: Decoder[Any, Any] = Decoder { x: Any => x }
}

object DoubleDecoderInstances extends BasicDecoders[Double] {
  override implicit def decodeToDouble = Decoder { d: Double => d }
  override implicit def decodeToInt = Decoder { d: Double => d.toInt }
  override implicit def decodeToString = Decoder { d: Double => d.toString }
  override implicit def decodeToAny = Decoder { d: Double => d }
}

// Hack for  String.toInt implicit method
object Helper {
  def strToInt(s: String) = s.toInt
}