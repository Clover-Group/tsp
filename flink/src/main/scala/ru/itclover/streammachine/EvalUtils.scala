package ru.itclover.streammachine

import java.io.File
import java.net.URLClassLoader
import java.util.jar.JarFile
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.PhaseParser
import scala.reflect.ClassTag

object EvalUtils {

  /**
    * Implementation on [[com.twitter.util.Eval]] with classloader as argument.
    */
  class Eval(classLoader: ClassLoader) extends com.twitter.util.Eval {
    override lazy val impliedClassPath: List[String] = {
      def getClassPath(cl: ClassLoader, acc: List[List[String]] = List.empty): List[List[String]] = {
        val cp = cl match {
          case urlClassLoader: URLClassLoader => urlClassLoader.getURLs.filter(_.getProtocol == "file").
            map(u => new File(u.toURI).getPath).toList
          case _ => Nil
        }
        cl.getParent match {
          case null => (cp :: acc).reverse
          case parent => getClassPath(parent, cp :: acc)
        }
      }

      val classPath = getClassPath(classLoader)
      val currentClassPath = classPath.head

      // if there's just one thing in the classpath, and it's a jar, assume an executable jar.
      currentClassPath ::: (if (currentClassPath.size == 1 && currentClassPath(0).endsWith(".jar")) {
        val jarFile = currentClassPath(0)
        val relativeRoot = new File(jarFile).getParentFile()
        val nestedClassPath = new JarFile(jarFile).getManifest.getMainAttributes.getValue("Class-Path")
        if (nestedClassPath eq null) {
          Nil
        } else {
          nestedClassPath.split(" ").map { f => new File(relativeRoot, f).getAbsolutePath }.toList
        }
      } else {
        Nil
      }) ::: classPath.tail.flatten
    }
  }

  def composePhaseCodeUsingRowExtractors(phaseCode: String, timestampField: Symbol, fieldsIndexesMap: Map[Symbol, Int]) = {
    s"""
       |import scala.concurrent.duration._
       |import ru.itclover.streammachine.core.Time._
       |import ru.itclover.streammachine.core._
       |import ru.itclover.streammachine.core.PhaseParser.Functions._
       |import ru.itclover.streammachine.phases.NumericPhases.SymbolParser
       |import ru.itclover.streammachine.phases.NumericPhases._
       |import ru.itclover.streammachine.phases.BooleanPhases._
       |import ru.itclover.streammachine.phases.ConstantPhases._
       |import ru.itclover.streammachine.phases.MonadPhases._
       |import ru.itclover.streammachine.phases.CombiningPhases._
       |import ru.itclover.streammachine.aggregators.AggregatorPhases._
       |import ru.itclover.streammachine.phases.Phases._
       |
       |import Predef.{any2stringadd => _, _}
       |import org.apache.flink.types.Row
       |
       |val fieldsIndexesMap: Map[Symbol, Int] = ${fieldsIndexesMap.toString}
       |
       |implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
       |  override def extract(event: Row, symbol: Symbol) = {
       |    event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Float].toDouble
       |  }
       |}
       |implicit val intSymbolExtractor = new SymbolExtractor[Row, Int] {
       |  override def extract(event: Row, symbol: Symbol): Int = event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Int]
       |}
       |implicit val longSymbolExtractor = new SymbolExtractor[Row, Long] {
       |  override def extract(event: Row, symbol: Symbol): Long = event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Long]
       |}
       |implicit val strSymbolExtractor = new SymbolExtractor[Row, String] {
       |  override def extract(event: Row, symbol: Symbol): String = event.getField(fieldsIndexesMap(symbol)).toString
       |}
       |implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
       |  override def apply(v1: Row) = {
       |    v1.getField(fieldsIndexesMap($timestampField)).asInstanceOf[java.sql.Timestamp]
       |  }
       |}
       |
       |val phase = $phaseCode
       |phase
      """.stripMargin
  }

}
