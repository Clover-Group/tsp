package ru.itclover.tsp.utils
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

case class EventCounterTrigger[T, W <: Window](maxElements: Long) extends Trigger[T, W] {
  var elements: Long = 0L

  override def onElement(
    element: T,
    timestamp: Long,
    window: W,
    ctx: Trigger.TriggerContext
  ): TriggerResult =
    if (elements < maxElements) {
      elements += 1
      TriggerResult.CONTINUE
    } else {
      elements = 0
      TriggerResult.FIRE_AND_PURGE
    }
  override def onProcessingTime(
    time: Long,
    window: W,
    ctx: Trigger.TriggerContext
  ): TriggerResult = TriggerResult.CONTINUE
  override def onEventTime(
    time: Long,
    window: W,
    ctx: Trigger.TriggerContext
  ): TriggerResult = TriggerResult.CONTINUE
  override def clear(
    window: W,
    ctx: Trigger.TriggerContext
  ): Unit = elements = 0
}
