package ru.itclover.tsp.streaming.utils

trait EventCreator[Event, Key, Schema] extends Serializable {
  def create(kv: Seq[(Key, AnyRef)], schema: Schema): Event
}
