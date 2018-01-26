package ru.itclover.streammachine.utils

import ru.itclover.streammachine.http.utils.ImplicitUtils._
import java.io.{File, FileNotFoundException}
import scala.io.Source
import scala.util.Try

object Files {
  def writeToFile(path: String, content: String): Try[Unit] = {
    val pw = new java.io.PrintWriter(new File(path))
    Try {
      pw.write(content)
    } eventually {
      pw.close()
    }
  }

  def readResource(resourcePath: String): Iterator[String] = {
    val stream = getClass.getResourceAsStream(resourcePath)
    scala.io.Source.fromInputStream(stream).getLines
  }

  def readFile(path: String): Try[String] = for {
    src <- Try(scala.io.Source.fromFile(path))
    str <- Try(src.mkString) eventually {
      src.close
    }
  } yield str
}