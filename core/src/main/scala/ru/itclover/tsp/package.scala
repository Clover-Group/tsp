package ru.itclover

import ru.itclover.tsp.core.Time
import ru.itclover.tsp.phases.CombiningPhases.And


package object tsp {

  case class Segment(from: Time, to: Time) extends Serializable

}
