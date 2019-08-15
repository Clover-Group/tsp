package ru.itclover.tsp.core.io

trait Decoder[-From, T] extends (From => T) with Serializable

object Decoder {

  def apply[From, T](f: From => T) = new Decoder[From, T] {
    override def apply(v1: From) = f(v1)
  }

  type AnyDecoder[T] = Decoder[Any, T]
}
