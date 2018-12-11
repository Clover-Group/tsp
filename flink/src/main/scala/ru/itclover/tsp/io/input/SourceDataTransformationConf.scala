package ru.itclover.tsp.io.input
import ru.itclover.tsp.io.{Extractors, KVExtractors}

sealed trait SourceDataTransformationConf

abstract class SourceDataTransformation[Event, EKey, EValue](val `type`: String) extends Serializable {
  val config: SourceDataTransformationConf
  // TODO
  def extractors: Extractors[Event, EKey, EValue]
  def kvExtractors: KVExtractors[Event, EKey, EValue]
}


case class NarrowDataUnfolding[Event, EKey, EValue](key: EKey, value: EValue, fieldsTimeoutsMs: Map[EKey, Long], defaultTimeout: Option[Long] = None)
   extends SourceDataTransformation[Event, EKey, EValue]("NarrowDataUnfolding") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
  override def extractors: Extractors[Event, EKey, EValue] = ???
  override def kvExtractors: KVExtractors[Event, EKey, EValue] = ???
}

case class WideDataFilling[Event, EKey, EValue](fieldsTimeoutsMs: Map[EKey, Long], defaultTimeout: Option[Long] = None)
  extends SourceDataTransformation[Event, EKey, EValue]("WideDataFilling") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
  override def extractors: Extractors[Event, EKey, EValue] = ???
  override def kvExtractors: KVExtractors[Event, EKey, EValue] = ???
}