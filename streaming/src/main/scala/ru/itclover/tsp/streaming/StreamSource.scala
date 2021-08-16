package ru.itclover.tsp.streaming

import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.streaming.io.{InputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.streaming.utils.{EventCreator, KeyCreator}

trait StreamSource[Event, EKey, EItem, EventSchema, Stream] extends Product with Serializable {

  def createStream: Stream

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Seq[String]

  def transformedPartitioner: Seq[String]

  def eventSchema: EventSchema

  def transformedEventSchema: EventSchema

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def transformedTimeExtractor: TimeExtractor[Event]

  implicit def idxExtractor: IdxExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]

  implicit def transformedExtractor: Extractor[Event, EKey, EItem]

  implicit def trivialEItemDecoder: Decoder[EItem, EItem] = (v1: EItem) => v1

  implicit def itemToKeyDecoder: Decoder[EItem, EKey] // for narrow data widening

  implicit def kvExtractor: Event => (EKey, EItem) = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, _, _)) =>
      (r: Event) => (extractor.apply[EKey](r, key), extractor.apply[EItem](r, value)) // TODO: See that place better
    case Some(WideDataFilling(_, _)) =>
      (_: Event) => sys.error("Wide data filling does not need K-V extractor")
    case Some(_) =>
      (_: Event) => sys.error("Unsupported data transformation")
    case None =>
      (_: Event) => sys.error("No K-V extractor without data transformation")
  }

  implicit def eventCreator: EventCreator[Event, EKey, EventSchema]

  implicit def keyCreator: KeyCreator[EKey]
}

object StreamSource {

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) =
    allFields.find { field =>
      !excludedFields.contains(field)
    }

  // todo: no vars
  var sparkMaster: String = "local"
}