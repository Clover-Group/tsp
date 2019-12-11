package ru.itclover.tsp.serializers.core

import ru.itclover.tsp.io.output.RowSchema

/**
  * Deserialization trait for Redis
  *
  * @tparam INPUT input type
  * @tparam OUTPUT output type
  */
trait Serialization[INPUT, OUTPUT] {

  def serialize(output: OUTPUT, rowSchema: RowSchema): INPUT
  def deserialize(input: INPUT, fieldsIdxMap: Map[Symbol, Int]): OUTPUT

}
