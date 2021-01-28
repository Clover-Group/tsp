package ru.itclover.tsp.http.utils

import org.scalatest.Matchers

// We use head and last instead of headOption and lastOption for performance reasons.
@SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
trait RangeMatchers extends Matchers {
  def beWithin(range: Range) = be >= range.head and be <= range.last
}
