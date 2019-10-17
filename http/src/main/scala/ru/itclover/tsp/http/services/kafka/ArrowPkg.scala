package ArrowPkg

// import org.apache.arrow.vector.types.Types.MinorType.{BIGINT, FLOAT8, INT, VARCHAR}
import org.apache.arrow.vector.{FieldVector}

trait ArrowOps {
  def unpack[A](din: FieldVector): Unit
}

object ArrowOps {

  def apply[A](implicit F: ArrowOps): ArrowOps = F

  implicit val IntOps = new ArrowOps {

    def unpack[INT](din: FieldVector) = {}

  }

  implicit val BigIntOps = new ArrowOps {

    def unpack[BIGINT](din: FieldVector) = {}

  }

  implicit val VCharOps = new ArrowOps {

    def unpack[VARCHAR](din: FieldVector) = {}
  }

  implicit val Float8Ops = new ArrowOps {

    def unpack[FLOAT8](din: FieldVector) = {}

  }

}
