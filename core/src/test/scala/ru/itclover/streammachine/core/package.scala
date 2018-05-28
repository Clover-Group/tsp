package ru.itclover.streammachine

import org.joda.time.{DateTime, Instant}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NoState
import ru.itclover.streammachine.phases.NumericPhases.{NumericPhaseParser, SymbolNumberExtractor}

package object core {

  case class TestEvent(i: Int, s: String)

  case class TimedEvent(i: Int, time: DateTime)


  case class TestingEvent[T](result: PhaseResult[T], time: DateTime = DateTime.now())

  implicit val doubleTestEvent = new TimeExtractor[TestingEvent[Double]] {
    override def apply(event: TestingEvent[Double]) = event.time
  }

  // TODO collapse
  implicit val boolTestEvent = new TimeExtractor[TestingEvent[Boolean]] {
    override def apply(event: TestingEvent[Boolean]) = event.time
  }

  case class TestPhase[T]() extends PhaseParser[TestingEvent[T], NoState, T] {
    override def initialState: NoState = NoState.instance

    override def apply(event: TestingEvent[T], state: NoState) = event.result -> NoState.instance

    override def format(event: TestingEvent[T], state: NoState) = s"TestPhase(${event})"
  }


  implicit val timeExtractor: TimeExtractor[TimedEvent] = new TimeExtractor[TimedEvent] {
    override def apply(v1: TimedEvent) = v1.time
  }

  val probe = TestEvent(1, "")

  val t = DateTime.now()
  val times = t.minusMillis(11000) :: t.minusMillis(10000) :: t.minusMillis(9000) :: t.minusMillis(8000) ::
    t.minusMillis(7000) :: t.minusMillis(6000) :: t.minusMillis(5000) :: t.minusMillis(4000) :: t.minusMillis(3000) ::
    t.minusMillis(2000) :: t.minusMillis(1000) :: t :: Nil

  private val staySuccessRes = Seq(Stay, Success(1.0), Stay, Success(2.0), Success(1.0), Success(3.0), Failure("Test"), Success(4.0))
  val staySuccesses = for((t, res) <- times.take(staySuccessRes.length).zip(staySuccessRes)) yield TestingEvent(res, t)

  def getTestingEvents[T](innerResults: Seq[PhaseResult[T]]) = for(
    (t, res) <- times.take(innerResults.length).zip(innerResults)
  ) yield TestingEvent(res, t)

  val failsRes = Seq(Stay, Success(1.0), Failure("Test"), Success(2.0), Failure("Test"), Success(1.0), Stay, Failure("Test"), Success(3.0), Failure("Test"), Stay)
  val fails = for((t, res) <- times.take(failsRes.length).zip(failsRes)) yield TestingEvent(res, t)



  class ConstantResult(result: PhaseResult[Int]) extends PhaseParser[TestEvent, Unit, Int] {
    override def apply(v1: TestEvent, v2: Unit): (PhaseResult[Int], Unit) = result -> ()

    override def initialState = ()
  }

  val alwaysSuccess = new ConstantResult(Success(0))
  val alwaysFailure = new ConstantResult(Failure("failed"))
  val alwaysStay = new ConstantResult(Stay)

  def fakeMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()

  def runRule[Event, Out](rule: PhaseParser[Event, _, Out], events: Seq[Event]): Vector[PhaseResult.TerminalResult[Out]] = {
    val mapResults = fakeMapper(rule)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

}
