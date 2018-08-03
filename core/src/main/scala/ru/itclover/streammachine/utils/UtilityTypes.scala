package ru.itclover.streammachine.utils

object UtilityTypes {
  final case class Url private(value: String) extends AnyVal

  type ThrowableOr[T] = Either[Throwable, T]

  case class ParseException(errs: Seq[String]) extends Throwable

  object ParseException {
    def apply(info: String): ParseException = ParseException(Seq(info))
  }
}
