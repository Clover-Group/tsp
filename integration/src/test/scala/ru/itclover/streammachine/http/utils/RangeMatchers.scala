package ru.itclover.streammachine.http.utils

import org.scalatest.Matchers

trait RangeMatchers extends Matchers {
  def beWithin(range: Range) = be >= range.head and be <= range.last
}
