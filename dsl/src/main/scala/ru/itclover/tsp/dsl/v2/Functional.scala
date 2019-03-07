package Functional

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
