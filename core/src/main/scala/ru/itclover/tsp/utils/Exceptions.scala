package ru.itclover.tsp.utils

import java.io.StringWriter
import java.io.PrintWriter

object Exceptions {
  case class InvalidRequest(msg: String) extends RuntimeException(msg)

  case class SourceException(msg: String) extends RuntimeException(msg)

  def getStackTrace(t: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    sw.toString
  }
}
