// Common objects for Testing

package ru.itclover.tsp.v2 

import Pattern._

// Dummy event 
sealed case class Event (ts:Long, col:Double)

final object Common {

  // Dummy event processing
  def procEvent(ev:Event):Long  = ev.ts 

  // Dummy extractor
  val ext  = new TsIdxExtractor(procEvent(_))

}


