// Common objects for Testing

package ru.itclover.tsp.v2 

import Pattern._

// Dummy event 
sealed case class Event[A] (ts:Long, row:A, col:A)


final object Common {

  type EInt = Event[Int]

  val event = Event[Int](0L, 0, 0)

  // Dummy event processing
  def procEvent(ev:EInt):Long  = ev.row

  // Dummy extractor
  val extractor = new TsIdxExtractor(procEvent(_))

}


