package ru.itclover.tsp.dsl

import cats.arrow.Arrow

import scala.language.higherKinds

// Generic Arrow Mapper
abstract class ArrMap[F[_, _], A, B] extends Arrow[F] {
  def eval: F[A, B]
}

// Logical Functions Typeclass
abstract class TLogical extends ArrMap[Map, (Symbol, Seq[ASTType]), (PFunction, ASTType)]

final object TLogical {

  type A = (Symbol, Seq[ASTType])
  type B = (PFunction, ASTType)

  def eval(a: A, b: B): Map[A, B] = Map(a -> b)

}

// Logical Functions typeclass
trait Logical[T] {

  def and(a: T, b: T): Boolean
  def or(a: T, b: T): Boolean
  def xor(a: T, b: T): Boolean
  def eq(a: T, b: T): Boolean
  def neq(a: T, b: T): Boolean
  def not(a: T): Boolean
}

// Here, we convert any incoming value to Boolean
@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object Logical {

  implicit val AnyLogical: Logical[Any] = new Logical[Any] {

    def and(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] && b.asInstanceOf[Boolean]
    def or(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] || b.asInstanceOf[Boolean]
    def xor(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] ^ b.asInstanceOf[Boolean]
    def eq(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] == b.asInstanceOf[Boolean] // FIXME ===
    def neq(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] != b.asInstanceOf[Boolean] // FIXME =!=
    def not(a: Any): Boolean = !a.asInstanceOf[Boolean]

  }
}
