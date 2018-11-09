package ru.itclover.tsp.io.input

sealed trait SourceDataTransformationConf

abstract class SourceDataTransformation(val `type`: String) extends Serializable {
  val config: SourceDataTransformationConf
}


case class NarrowDataUnfolding(key: Symbol, value: Symbol, fieldsTimeoutsMs: Map[Symbol, Long], defaultTimeout: Option[Long] = None)
   extends SourceDataTransformation("NarrowDataUnfolding") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
}

case class WideDataFilling(fieldsTimeoutsMs: Map[Symbol, Long], defaultTimeout: Option[Long] = None)
  extends SourceDataTransformation("WideDataFilling") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
}