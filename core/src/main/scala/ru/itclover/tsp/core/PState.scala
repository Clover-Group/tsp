package ru.itclover.tsp.core

import ru.itclover.tsp.core.Pattern.QI

trait PState[T, +Self <: PState[T, _]] extends Replacable[QI[T], Self]{
  def queue: QI[T]
  def copyWith(queue: QI[T]): Self
}

trait AnyState[T] extends PState[T, AnyState[T]]

trait Replacable[T, +S]{
  def copyWith(t: T): S
}