package ru.itclover.streammachine


import java.time.Instant
import ru.itclover.streammachine.core.PhaseParser.And
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Terminate}
import ru.itclover.streammachine.core.PhaseResult._
import ru.itclover.streammachine.core.Time.TimeExtractor


//trait ResultsParser[Event, State, Out] extends ((Event, Seq[PhaseResult[Out] And State]) => (Seq[PhaseResult[Out] And State]))


//case class ConcatSegmentsResultsParser[State]()
//  extends ResultsParser[_, State, (Instant, Instant)] {
//  // TODO Add gap interval `maxGapInterval: TimeInterval`
//
//  case class Segment(begin: Instant, end: Instant) {
//    def concat(other: Segment) = {
//      // TODO: if other.start > start ??? else ???
//      this :: other :: Nil
//    }
//  }
//
//  var segment: Option[Segment] = None
//
//  override def apply(v1: Event, resultsSegmentsAndStates: Seq[(PhaseResult[(Instant, Instant)], State)]) = {
//    val (terminal, nonTerminal) = resultsSegmentsAndStates.partition(_._1.isTerminal)
//
//    val a = terminal.filter {
//      case (Success(_), _) => true
//      case (_, _) => false
//    } map { successAndState =>
//      val (Success((begin, end)), _) = successAndState
//      segment match {
//        case Some(segment) => Segment(begin, end).concat(segment)
//        case None => Segment(begin, end) :: Nil
//      }
//    }
//    a
//  }
//}


//case class TerminateResultsParser[Out, State]() extends ResultsParser[Out, State] {
//  override def apply(resultsAndStates: Seq[PhaseResult[Out] And State]) = {
//    resultsAndStates
//  }
//}


trait AbstractStateMachineMapper[Event, State, Out] {

  def phaseParser: PhaseParser[Event, State, Out]

  // def phaseResultParser: ResultsParser[PhaseResult[Out], State]

  // we terminate our parser. So if it got to the Terminal state if will be stay there forever.
  def terminatedParser = Terminate(phaseParser)

  def process(event: Event, oldStates: Seq[State]): (Seq[TerminalResult[Out]], Seq[State]) = {

    // new state is adding every time to account every possible outcomes considering terminal nature of phase mappers
    val oldStatesWithOneInitialState = oldStates :+ phaseParser.initialState

    val stateToResult = phaseParser.curried(event)

    val resultsAndStates = oldStatesWithOneInitialState.map(stateToResult)

    val (toEmit, newStates) = resultsAndStates.span(_._1.isTerminal)

    toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
  }


  def getTerminalResultsAndNewStates(resultsAndStates: Seq[(PhaseResult[Out], State)]) = {

  }

}