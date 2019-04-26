package ru.itclover.tsp.core.io

trait BasicDecoders[From] {
  implicit def decodeToDouble: Decoder[From, Double]
  implicit def decodeToInt: Decoder[From, Int]
  implicit def decodeToString: Decoder[From, String]
  implicit def decodeToAny: Decoder[From, Any]
}

object AnyDecodersInstances extends BasicDecoders[Any] with Serializable {
  import Decoder._

  implicit val decodeToDouble: Decoder[Any, Double] = new AnyDecoder[Double] {
    override def apply(x: Any) = x match {
      case d: Double           => d
      case n: java.lang.Number => n.doubleValue()
      case s: String =>
        try { Helper.strToDouble(s) } catch {
          case e: Exception =>
            throw new RuntimeException(s"Cannot parse String ($s) to Double, exception: ${e.toString}")
        }
      case null => Double.NaN
    }
  }

  implicit val decodeToInt: Decoder[Any, Int] = new AnyDecoder[Int] {
    override def apply(x: Any) = x match {
      case i: Int              => i
      case n: java.lang.Number => n.intValue()
      case s: String =>
        try { Helper.strToInt(s) } catch {
          case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
        }
      case null => throw new RuntimeException(s"Cannot parse null to Int")
    }
  }

  implicit val decodeToLong: Decoder[Any, Long] = new AnyDecoder[Long] {
    override def apply(x: Any) = x match {
      case i: Int              => i
      case l: Long             => l
      case n: java.lang.Number => n.longValue()
      case s: String =>
        try { Helper.strToInt(s) } catch {
          case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
        }
      case null => throw new RuntimeException(s"Cannot parse null to Long")
    }
  }

  implicit val decodeToBoolean: Decoder[Any, Boolean] = new AnyDecoder[Boolean] {
    override def apply(x: Any) = x match {
      case 0 | 0L | 0.0 | "0" | "false" | "off" | "no" => false
      case 1 | 1L | 1.0 | "1" | "true" | "on" | "yes"  => true
      case b: Boolean                                  => b
      case null                                        => throw new RuntimeException(s"Cannot parse null to Boolean")
      case _                                           => throw new RuntimeException(s"Cannot parse '$x' to Boolean")
    }
  }

  implicit val decodeToString: Decoder[Any, String] = Decoder { x: Any =>
    x.toString
  }

  implicit val decodeToAny: Decoder[Any, Any] = Decoder { x: Any =>
    x
  }
}

object DoubleDecoderInstances extends BasicDecoders[Double] {
  implicit override def decodeToDouble: Decoder[Double, Double] = Decoder { d: Double =>
    d
  }
  implicit override def decodeToInt: Decoder[Double, Int] = Decoder { d: Double =>
    d.toInt
  }
  implicit override def decodeToString: Decoder[Double, String] = Decoder { d: Double =>
    d.toString
  }
  implicit override def decodeToAny: Decoder[Double, Any] = Decoder { d: Double =>
    d
  }
}

// Hack for String.toInt implicit method
object Helper {
  def strToInt(s: String) = s.toInt
  def strToDouble(s: String) = s.toDouble
}
