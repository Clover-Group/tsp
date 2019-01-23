package ru.itclover.tsp.v2

import ru.itclover.tsp.v2.Pattern.QI

trait PState[T, +Self <: PState[T, _]] {
  def queue: QI[T]
  def copyWithQueue(queue: QI[T]): Self
}

trait AnyState[T] extends PState[T, AnyState[T]]
