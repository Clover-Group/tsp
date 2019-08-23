package ArrowPkg

import org.apache.arrow.vector.types.Types.MinorType.{INT, BIGINT, FLOAT8, VARCHAR}
import org.apache.arrow.vector.{FieldVector}

trait ArrowOps[A] {
  def unpack[A](din: FieldVector): Unit
}

object ArrowOps {

  def apply[A](implicit F: ArrowOps[A]): ArrowOps[A] = F

  implicit val IntOps = new ArrowOps[INT.type] {

    def unpack[INT](din: FieldVector) = {}

  }

  implicit val BigIntOps = new ArrowOps[BIGINT.type] {

    def unpack[BIGINT](din: FieldVector) = {}

  }

  implicit val VCharOps = new ArrowOps[VARCHAR.type] {

    def unpack[VARCHAR](din: FieldVector) = {}
  }

  implicit val Float8Ops = new ArrowOps[FLOAT8.type] {

    def unpack[FLOAT8](din: FieldVector) = {}

  }

}
