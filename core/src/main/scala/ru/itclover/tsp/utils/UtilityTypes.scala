package ru.itclover.tsp.utils

object UtilityTypes {

  type And[+L, +R] = (L, R)

  type Or[L, R] = Either[L, R]


  final case class Url private(value: String) extends AnyVal


  type ThrowableOr[T] = Either[Throwable, T]

  case class ParseException(errs: Seq[String]) extends Throwable

  object ParseException {
    def apply(info: String): ParseException = ParseException(Seq(info))
  }
}
