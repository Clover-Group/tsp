package ru.itclover.streammachine.phases

/**
  * I know it's awkward. But don't change it. It has written the way how it is written
  * because of spark's encoders construction.
  */
case class NoState private() extends Serializable

object NoState {
  val instance = new NoState()
}