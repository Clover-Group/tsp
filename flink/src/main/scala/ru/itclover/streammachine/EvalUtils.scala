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

  def composePhaseCodeUsingRowExtractors(phaseCode: String, tsField: Symbol, fieldsIdxMap: Map[Symbol, Int]) = {
    // Partial fix for fancy column names form DB, eg `some(123)`
    val fieldsIdxMapStr = fieldsIdxMap.map { case (f, i) => (s"""Symbol("${f.toString.tail}")""", i) }.toString
    s"""
       |import scala.util.Try
       |import java.math.BigInteger
       |import java.time.Instant
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
       |import ru.itclover.streammachine.aggregators._
       |import ru.itclover.streammachine.phases.Phases._
       |import ru.itclover.streammachine.core.Time.{DoubleTimeLike, BigIntTimeLike}
       |
       |import Predef.{any2stringadd => _, _}
       |import org.apache.flink.types.Row
       |
       |val fieldsIdxMap: Map[Symbol, Int] = $fieldsIdxMapStr
       |
       |implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
       |  override def extract(event: Row, symbol: Symbol) = {
       |    event.getField(fieldsIdxMap(symbol)) match {
       |      case d: java.lang.Double => d.doubleValue()
       |      case f: java.lang.Float => f.floatValue().toDouble
       |      case some => Try(some.toString.toDouble).getOrElse(
       |          throw new ClassCastException(s"Cannot cast value $$some to double."))
       |    }
       |  }
       |}
       |implicit val intSymbolExtractor = new SymbolExtractor[Row, Int] {
       |  override def extract(event: Row, symbol: Symbol): Int = event.getField(fieldsIdxMap(symbol)) match {
       |    case value: java.lang.Integer => value.intValue()
       |    case some => Try(some.toString.toInt).getOrElse(
       |          throw new ClassCastException(s"Cannot cast value $$some to int."))
       |  }
       |}
       |implicit val longSymbolExtractor = new SymbolExtractor[Row, Long] {
       |  override def extract(event: Row, symbol: Symbol): Long = event.getField(fieldsIdxMap(symbol)) match {
       |    case value: java.lang.Long => value.longValue()
       |    case some => Try(some.toString.toLong).getOrElse(
       |          throw new ClassCastException(s"Cannot cast value $$some to long."))
       |  }
       |}
       |implicit val strSymbolExtractor = new SymbolExtractor[Row, String] {
       |  override def extract(event: Row, symbol: Symbol): String = event.getField(fieldsIdxMap(symbol)).toString
       |}
       |implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
       |  override def apply(event: Row) = event.getField(fieldsIdxMap($tsField)) match {
       |    case isoTime: String => Instant.parse(isoTime).toEpochMilli / 1000.0
       |    case epochMillis => epochMillis.asInstanceOf[Double]
       |  }
       |}
       |
       |val phase = $phaseCode
       |phase
      """.stripMargin
  }

}
