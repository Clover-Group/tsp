package ru.itclover.tsp.services

import org.apache.arrow.vector.{IntVector, VarCharVector, BitVector, Float4Vector, Float8Vector}
import org.apache.arrow.vector.types.Types

object ArrowService {

  /**
  * Method for working with types from Apache Arrow schema.
  * @return
  */
  def typesMap = Map(
    Types.MinorType.BIGINT -> (classOf[Int], classOf[IntVector]),
    Types.MinorType.SMALLINT -> (classOf[Int], classOf[IntVector]),
    Types.MinorType.BIT -> (classOf[Boolean], classOf[BitVector]),
    Types.MinorType.INT -> (classOf[Int], classOf[IntVector]),
    Types.MinorType.VARCHAR -> (classOf[String], classOf[VarCharVector]),
    Types.MinorType.FLOAT4 -> (classOf[Float], classOf[Float4Vector]),
    Types.MinorType.FLOAT8 -> (classOf[Double], classOf[Float8Vector])
  )

}
