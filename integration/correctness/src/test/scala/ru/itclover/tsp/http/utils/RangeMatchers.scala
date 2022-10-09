package ru.itclover.tsp.http.utils

import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should._

// Using `head` and `last` for performance reason
@SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
trait RangeMatchers extends Matchers {
  def beWithin(range: Range): Matcher[Int] = be >= range.head and be <= range.last
}
