package ru.itclover.streammachine.utils

import com.typesafe.scalalogging.Logger

object Time {

  def timeIt[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    ((t1 - t0) / 10e9, result)
  }
}
