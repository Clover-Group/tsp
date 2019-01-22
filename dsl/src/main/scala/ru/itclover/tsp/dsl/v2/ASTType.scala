package ru.itclover.tsp.dsl.v2
import scala.reflect.ClassTag

trait ASTType

case object IntASTType extends ASTType
case object LongASTType extends ASTType
case object BooleanASTType extends ASTType
case object DoubleASTType extends ASTType
case object AnyASTType extends ASTType

object ASTType {

  def of[T](implicit ct: ClassTag[T]): ASTType = ct.runtimeClass match {
    case c if c.isAssignableFrom(classOf[Double])  => DoubleASTType
    case c if c.isAssignableFrom(classOf[Long])    => LongASTType
    case c if c.isAssignableFrom(classOf[Int])     => IntASTType
    case c if c.isAssignableFrom(classOf[Boolean]) => BooleanASTType
    case _                                         => AnyASTType
  }
}