package ru.itclover

import ru.itclover.streammachine.core.Time
import ru.itclover.streammachine.phases.CombiningPhases.And


package object streammachine {

  case class Segment(from: Time, to: Time) extends Serializable

}
