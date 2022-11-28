package ru.itclover.tsp.dsl

import scala.reflect.ClassTag
import com.typesafe.scalalogging.Logger

trait ASTType

case object IntASTType extends ASTType
case object LongASTType extends ASTType
case object BooleanASTType extends ASTType
case object DoubleASTType extends ASTType
case object StringASTType extends ASTType
case object NullASTType extends ASTType
case object AnyASTType extends ASTType

object ASTType {

  val log = Logger("ASTType")

  def of[T](implicit ct: ClassTag[T]): ASTType = ct.runtimeClass match {
    // Basic check, if T isn't lost
    case c if c.isAssignableFrom(classOf[Double])  => DoubleASTType
    case c if c.isAssignableFrom(classOf[Long])    => LongASTType
    case c if c.isAssignableFrom(classOf[Int])     => IntASTType
    case c if c.isAssignableFrom(classOf[Short])   => IntASTType
    case c if c.isAssignableFrom(classOf[Byte])    => IntASTType
    case c if c.isAssignableFrom(classOf[Boolean]) => BooleanASTType
    case c if c.isAssignableFrom(classOf[String])  => StringASTType
    case c if c.isAssignableFrom(classOf[Nothing]) => NullASTType

    // Extra check, in case type T is lost
    case c
        if isNamesMatch(
          c,
          Seq(Double.getClass, Float.getClass, classOf[java.lang.Double], classOf[java.lang.Float])
        ) =>
      DoubleASTType
    case c if isNamesMatch(c, Seq(Long.getClass, classOf[java.lang.Long]))       => LongASTType
    case c if isNamesMatch(c, Seq(Int.getClass, classOf[java.lang.Integer]))     => IntASTType
    case c if isNamesMatch(c, Seq(Short.getClass, classOf[java.lang.Short]))     => IntASTType
    case c if isNamesMatch(c, Seq(Byte.getClass, classOf[java.lang.Byte]))       => IntASTType
    case c if isNamesMatch(c, Seq(Boolean.getClass, classOf[java.lang.Boolean])) => BooleanASTType
    case c if isNamesMatch(c, Seq(classOf[java.lang.String]))                    => StringASTType

    case _ => {
      log.warn(s"${ct.runtimeClass.getName()} is not a known class, defaulting to AnyASTType")
      AnyASTType
    }
  }

  private def isNamesMatch(tag: Class[_], classes: Seq[Class[_]]) =
    classes.map(_.getName).contains(tag.getName)
}
