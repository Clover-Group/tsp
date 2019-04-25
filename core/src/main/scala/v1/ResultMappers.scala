//package ru.itclover.tsp
//
//import ru.itclover.tsp.core.PatternResult.{Success, TerminalResult}
//
///**
//  * Used for statefully process result inside of each [[PatternMapper.apply]] with Event.
// *
//  * @tparam Event     - inner Event
//  * @tparam PhaseOut  - result of [[PatternMapper.apply]]
//  * @tparam MapperOut - resulting results sequence
//  */
//trait ResultMapper[Event, PhaseOut, MapperOut] extends
//  ((Event, Seq[TerminalResult[PhaseOut]]) => Seq[TerminalResult[MapperOut]]) with Serializable
//
//
//object ResultMapper {
//
//  implicit class ResultMapperRich[Event, PhaseOut, MapperOut](val mapper: ResultMapper[Event, PhaseOut, MapperOut]) extends AnyVal {
//
//    def andThen[Mapper2Out](secondMapper: ResultMapper[Event, MapperOut, Mapper2Out]):
//      AndThenResultsMapper[Event, PhaseOut, MapperOut, Mapper2Out] = AndThenResultsMapper(mapper, secondMapper)
//  }
//}
//
//
//case class FakeMapper[Event, PhaseOut]() extends ResultMapper[Event, PhaseOut, PhaseOut] {
//  def apply(event: Event, results: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[PhaseOut]] = results
//}
//
//
//case class AndThenResultsMapper[Event, PhaseOut, Mapper1Out, Mapper2Out](first: ResultMapper[Event, PhaseOut, Mapper1Out],
//                                                                         second: ResultMapper[Event, Mapper1Out, Mapper2Out])
//  extends ResultMapper[Event, PhaseOut, Mapper2Out] {
//
//  override def apply(e: Event, r: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[Mapper2Out]] = second(e, first(e, r))
//}
