package ru.itclover.streammachine.utils

object UtilityTypes {
  final case class Url private(value: String) extends AnyVal

  type ThrowableOr[T] = Either[Throwable, T]
}
