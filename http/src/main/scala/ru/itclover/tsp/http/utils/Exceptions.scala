package ru.itclover.tsp.http.utils

import java.io.{PrintWriter, StringWriter}

object Exceptions {
  case class InvalidRequest(msg: String) extends RuntimeException(msg)

  def getStackTrace(t: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    sw.toString
  }
}
