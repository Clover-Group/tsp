package ru.itclover.streammachine.utils

import java.io.StringWriter
import java.io.PrintWriter

object Exceptions {
  def getStackTrace(t: Throwable): String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      t.printStackTrace(pw)
      sw.toString
  }
}
