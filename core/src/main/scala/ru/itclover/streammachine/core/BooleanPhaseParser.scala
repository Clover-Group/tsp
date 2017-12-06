//package ru.itclover.streammachine.core
//
//import ru.itclover.streammachine.core.PhaseResult.Success
//
//trait BooleanPhaseParser[Event, S] extends PhaseParser[Event, S, Boolean] {
//
//
//}
//
//
///**
//  * PhaseParser returning only Success(true), Failure and Stay. Cannot return Success(false)
//  *
//  * @tparam Event - event type
//  * @tparam S     - possible inner state
//  */
//trait AssertPhaseParser[Event, S] extends BooleanPhaseParser[Event, S] {
//
//
//}
//
//object AssertPhaseParser {
//
//
//
//  case object SuccessTrue extends Success[Boolean](true)
//
//  def assert[Event, State](condition: BooleanPhaseParser[Event, State]) = new AssertPhaseParser[Event, State] {
//    override def initialState: State = condition.initialState
// condition.flatMap(b => if(b) )
//    override def apply(v1: Event, v2: State) = {
//     val (innerResult, innerNewState) = condition(v1, v2)
//
//
//    }
//  }
//
//}