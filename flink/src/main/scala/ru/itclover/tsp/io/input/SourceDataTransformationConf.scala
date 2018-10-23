package ru.itclover.tsp.io.input

sealed trait SourceDataTransformationConf

abstract class SourceDataTransformation(val `type`: String) extends Serializable {
  val config: SourceDataTransformationConf
}


case class NarrowDataUnfolding(key: Symbol, value: Symbol, fieldsTimeouts: Map[Symbol, Long])
   extends SourceDataTransformation("NarrowDataUnfolding") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
}
