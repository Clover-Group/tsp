package ru.itclover.tsp.core

import ru.itclover.tsp.core.Pattern.QI

trait PState[T, +Self <: PState[T, _]] {
  def queue: QI[T]
  def copyWith(queue: QI[T]): Self
}

trait AnyState[T] extends PState[T, AnyState[T]]