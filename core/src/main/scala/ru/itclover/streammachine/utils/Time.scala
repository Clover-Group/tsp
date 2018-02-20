package ru.itclover.streammachine.utils

import com.typesafe.scalalogging.Logger

object Time {
  private val log = Logger("Time")

  def timeIt[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    log.info("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}
