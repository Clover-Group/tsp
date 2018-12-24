package ru.itclover

import ru.itclover.tsp.core.Time


package object tsp {

  case class Segment(from: Time, to: Time) extends Serializable

}
