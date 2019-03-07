package Functional

import scala.language.higherKinds
import cats.arrow.Arrow

import ru.itclover.tsp.dsl.v2.{ASTType, PFunction}

// Generic Arrow Mapper
sealed abstract class ArrMap[F[_,_], A,B] extends Arrow[F] {
  def eval:F[A,B]
}

// Logical Functions Typeclass
sealed abstract class TLogical extends ArrMap[Map, (Symbol, Seq[ASTType]), (PFunction, ASTType)]

final object TLogical {

  type A = (Symbol, Seq[ASTType])
  type B = (PFunction, ASTType)

  def eval (a:A, b:B):Map[A,B] = Map(a -> b)

}


// Logical Functions typeclass
trait Logical[T] {

  def and (a:T, b:T):Boolean
  def or  (a:T, b:T):Boolean
  def xor (a:T, b:T):Boolean
  def eq  (a:T, b:T):Boolean
  def neq (a:T, b:T):Boolean
  def not (a:T)     :Boolean
}
 
object Logical {

  implicit val AnyLogical:Logical[Any]  = new Logical[Any] {

    def and (a:Any, b:Any):Boolean = a.asInstanceOf[Boolean] && b.asInstanceOf[Boolean]
    def or  (a:Any, b:Any):Boolean = a.asInstanceOf[Boolean] || b.asInstanceOf[Boolean]
    def xor (a:Any, b:Any):Boolean = a.asInstanceOf[Boolean] ^  b.asInstanceOf[Boolean]
    def eq  (a:Any, b:Any):Boolean = a.asInstanceOf[Boolean] == b.asInstanceOf[Boolean] // FIXME ===
    def neq (a:Any, b:Any):Boolean = a.asInstanceOf[Boolean] != b.asInstanceOf[Boolean] // FIXME =!=
    def not (a:Any)       :Boolean = !a.asInstanceOf[Boolean] 

  }
}
