// Common objects for Testing

package ru.itclover.tsp.v2 

import Pattern._

// Dummy event 
sealed case class Event[A] (ts:Long, row:A, col:A)

final object Common {

  // Dummy event processing
  def procEvent(ev:Event[Int]):Long  = ev.ts 

  // Dummy extractor
  val extractor = new TsIdxExtractor(procEvent(_))

}


