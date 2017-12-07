//package ru.itclover.streammachine.vectorState
//
//import ru.itclover.streammachine.core.AliasedParser.Aliased
//import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}
//import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
//import ru.itclover.streammachine.vectorState.VectorPhaseParser.{And, AndThen, Or, Packed}
//
//import scala.language.higherKinds
//
///**
//  * Base trait. Used for statefully processing Event's. In each step returns some PhaseResult and new State
//  *
//  * @tparam Event - events to process
//  * @tparam State - inner state
//  * @tparam T     - output type, used if phase successfully terminated
//  */
//trait VectorPhaseParser[Event, State, +T] extends ((Event, Packed[State]) => (PhaseResult[T], Packed[State])) {
//  def initialState: State
//
//  def size: Int = 1
//}
//
//object VectorPhaseParser {
//
//  type Packed[State] = (Vector[Any], Int)
//
//  object Packed {
//    def pack[State](vector: Vector[Any], i: Int): Packed[State] = (vector, i)
//  }
//
//  /**
//    * Type alias to use in AndParser
//    *
//    * @tparam L
//    * @tparam R
//    */
//  type And[L, R] = (L, R)
//
//  /**
//    * Type alias to use in OrParser
//    *
//    * @tparam L
//    * @tparam R
//    */
//  type Or[L, R] = Either[L, R]
//  /**
//    * Type alias to use in AndThenParser
//    *
//    * @tparam L
//    * @tparam R
//    */
//  type AndThen[L, R] = (L, R)
//
//  implicit class PhaseParserRich[Event, State, T](val parser: VectorPhaseParser[Event, State, T]) extends AnyVal {
//
//    /**
//      * Allows easily create AndParser like:
//      * `parser1 and parser2`
//      *
//      * @param rightParser - phase parser to add.
//      * @return new AndParser
//      */
//    def and[RightState, RightOut](rightParser: VectorPhaseParser[Event, RightState, RightOut]):
//    VectorAndParser[Event, State, RightState, T, RightOut] = VectorAndParser(parser, rightParser)
//
//    /**
//      * Alias for `and`
//      */
//    def &[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
//    AndParser[Event, State, RightState, T, RightOut] = and(rightParser)
//
//    /**
//      * Allows easily create LazyAndParser like:
//      * `parser1 lazyAnd parser2`
//      *
//      * @param rightParser - phase parser to add.
//      * @return new AndParser
//      */
//    def lazyAnd[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
//    LazyAndParser[Event, State, RightState, T, RightOut] = LazyAndParser(parser, rightParser)
//
//    /**
//      * Alias for `lazyAnd`
//      */
//    def &&[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
//    LazyAndParser[Event, State, RightState, T, RightOut] = lazyAnd(rightParser)
//
//    /**
//      * Allows easily create AndThenParser like:
//      * `parser1 andThen parser2`
//      *
//      * @param nextParser - phase parser to add after this one
//      * @return new AndThenParser
//      */
//    def andThen[NextState, NextOut](nextParser: PhaseParser[Event, NextState, NextOut]):
//    AndThenParser[Event, State, NextState, T, NextOut] = AndThenParser(parser, nextParser)
//
//    /**
//      * alias for andThen method
//      */
//    def ~[NextState, NextOut](nextParser: PhaseParser[Event, NextState, NextOut]):
//    AndThenParser[Event, State, NextState, T, NextOut] = andThen(nextParser)
//
//    /**
//      * Allows easily create OrParser like:
//      * `parser1 or parser2`
//      *
//      * @param rightParser - phase parser to add.
//      * @return new OrParser
//      */
//    def or[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
//    OrParser[Event, State, RightState, T, RightOut] = OrParser(parser, rightParser)
//
//    /**
//      * Alias for `or`
//      */
//    def |[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
//    OrParser[Event, State, RightState, T, RightOut] = or(rightParser)
//
//    def timed(timeInterval: TimeInterval)(implicit timeExtractor: TimeExtractor[Event]) = parser and Timer(timeInterval)
//
//    def until[State2](condition: PhaseParser[Event, State2, Boolean]) = UntilParser(this, condition)
//
//    def mapWithEvent[B](f: (Event, T) => B): MapWithEventParser[Event, State, T, B] = MapWithEventParser(parser, f.curried)
//
//    def map[B](f: T => B): MapParser[Event, State, T, B] = MapParser(parser, f)
//
//    def flatMap[State2, Out2](f: T => PhaseParser[Event, State2, Out2]) = FlatMapParser(parser, f)
//  }
//
//}
//
///**
//  * Parser combining two other parsers.
//  * Uses the following rules:
//  * Success only if both parts are Success.
//  * Failure if any of sides is Failure
//  * Else Stay
//  */
//case class VectorAndParser[Event, LState, RState, LOut, ROut]
//(
//  leftParser: VectorPhaseParser[Event, LState, LOut],
//  rightParser: VectorPhaseParser[Event, RState, ROut]
//)
//  extends VectorPhaseParser[Event, LState And RState, LOut And ROut] {
//
//  override def apply(event: Event, state: Packed[LState And RState]): (PhaseResult[LOut And ROut], Packed[LState And RState]) = {
//
//    val vector = state._1
//    val idx = state._2
//
//    val leftState = Packed.pack(vector, idx)
//
//    val (leftResult, newLeftState) = leftParser(event, leftState)
//    val rightState = Packed.pack(newLeftState._1, idx + leftParser.size)
//    val (rightResult, newRightState) = rightParser(event, rightState)
//    val newState = newRightState
//    ((leftResult, rightResult) match {
//      case (Success(leftOut), Success(rightOut)) => Success(leftOut -> rightOut)
//      case (Failure(msg), _) => Failure(msg)
//      case (_, Failure(msg)) => Failure(msg)
//      case (_, _) => Stay
//    }) -> newState
//  }
//
//  override def initialState: LState And RState = leftParser.initialState -> rightParser.initialState
//}
//
//
//case class AliasedParser[Event, InnerState, InnerOut](innerParser: PhaseParser[Event, InnerState, InnerOut], alias: String)
//  extends PhaseParser[Event, InnerState, Aliased[InnerOut]] {
//
//  val curriedAlias: InnerOut => Aliased[InnerOut] = (Aliased.apply[InnerOut] _).curried(alias)
//
//  override def apply(event: Event, state: InnerState): (PhaseResult[Aliased[InnerOut]], InnerState) = {
//    val (result, newState) = innerParser(event, state)
//    // TODO inherit from Success
//    result.map(curriedAlias) -> newState
//  }
//
//  override def initialState: (InnerState) = innerParser.initialState
//}
//
//object AliasedParser {
//
//  case class Aliased[T](alias: String, value: T)
//
//}
//
//
//// todo think about using shapeless here.
///**
//  * PhaseParser chaining two parsers one after another.
//  * Success if first phase finished successfully and second is successfull too.
//  * Failure if any of phases finished with Failure.
//  * If first phase is in Stay or first phase has finished but second in Stay the result is Stay.
//  */
//case class AndThenParser[Event, FirstState, SecondState, FirstOut, SecondOut]
//(
//  first: PhaseParser[Event, FirstState, FirstOut],
//  second: PhaseParser[Event, SecondState, SecondOut]
//)
//  extends PhaseParser[Event, (FirstState, SecondState, Option[FirstOut]), FirstOut AndThen SecondOut] {
//
//  override def apply(event: Event, state: (FirstState, SecondState, Option[FirstOut])):
//  (PhaseResult[FirstOut AndThen SecondOut], (FirstState, SecondState, Option[FirstOut])) = {
//
//    // third element in state is the result of first phase. Contains None if first phase was not finished
//    val (firstState, secondState, optFirstOut) = state
//
//    optFirstOut match {
//      case None =>
//        val (firstResult, newFirstState) = first(event, firstState)
//        firstResult match {
//          case Success(firstOut) => {
//            //we should try to reapply this event to the second phase
//            val (secondResult, newSecondState) = second(event, secondState)
//            val newState = (newFirstState, newSecondState, Some(firstOut))
//            (secondResult match {
//              case Success(secondOut) => Success(firstOut -> secondOut)
//              case f@Failure(msg) => f
//              case Stay => Stay
//            }) -> newState
//          }
//          case f@Failure(msg) => f -> (newFirstState, secondState, None)
//          case Stay => Stay -> (newFirstState, secondState, None)
//        }
//      case Some(firstOut) =>
//        val (secondResult, newSecondState) = second(event, secondState)
//
//        (secondResult match {
//          case Success(secondOut) => Success(firstOut -> secondOut)
//          case f@Failure(msg) => f
//          case Stay => Stay
//        }) -> (firstState, newSecondState, optFirstOut)
//    }
//  }
//
//  override def initialState = (first.initialState, second.initialState, None)
//}
//
//case class OrParser[Event, LState, RState, LOut, ROut]
//(
//  leftParser: PhaseParser[Event, LState, LOut],
//  rightParser: PhaseParser[Event, RState, ROut]
//)
//  extends PhaseParser[Event, (LState, RState), LOut Or ROut] {
//
//  override def apply(event: Event, state: (LState, RState)): (PhaseResult[LOut Or ROut], (LState, RState)) = {
//    val (leftState, rightState) = state
//
//    val (leftResult, newLeftState) = leftParser(event, leftState)
//    val (rightResult, newRightState) = rightParser(event, rightState)
//    val newState = newLeftState -> newRightState
//    ((leftResult, rightResult) match {
//      case (Success(leftOut), _) => Success(Left(leftOut))
//      case (_, Success(rightOut)) => Success(Right(rightOut))
//      case (Stay, _) => Stay
//      case (_, Stay) => Stay
//      case (Failure(msg1), Failure(msg2)) => Failure(s"Or Failed: 1) $msg1 2) $msg2")
//    }) -> newState
//  }
//
//  override def initialState = (leftParser.initialState, rightParser.initialState)
//}
//
///**
//  * Parser combining two other parsers. The second parser starts only if the first has finished succesfully at least once.
//  * Uses the following rules:
//  * Success only if both parts are Success.
//  * Only in that case try second parser.
//  * Failure if any of sides is Failure
//  * Else Stay
//  */
//case class LazyAndParser[Event, LState, RState, LOut, ROut]
//(
//  leftParser: PhaseParser[Event, LState, LOut],
//  rightParser: PhaseParser[Event, RState, ROut]
//)
//  extends PhaseParser[Event, LState And RState, LOut And ROut] {
//
//  override def apply(event: Event, state: LState And RState): (PhaseResult[LOut And ROut], LState And RState) = {
//    val (leftState, rightState) = state
//
//    val (leftResult, newLeftState) = leftParser(event, leftState)
//    leftResult match {
//      case (Success(leftOut)) => {
//        val (rightResult, newRightState) = rightParser(event, rightState)
//        val newState = newLeftState -> newRightState
//        (rightResult match {
//          case Success(rightOut) => Success(leftOut -> rightOut)
//          case f@Failure(msg) => Failure(msg)
//          case Stay => Stay
//        }) -> newState
//      }
//      case f@Failure(msg) => f -> (newLeftState -> rightState)
//      case Stay => Stay -> (newLeftState -> rightState)
//    }
//  }
//
//  override def initialState = (leftParser.initialState, rightParser.initialState)
//}
//
//
//case class MapWithEventParser[Event, State, In, Out](phaseParser: PhaseParser[Event, State, In], f: Event => In => Out) extends PhaseParser[Event, State, Out] {
//
//  override def apply(event: Event, oldState: State): (PhaseResult[Out], State) = {
//    val (phaseResult, state) = phaseParser.apply(event, oldState)
//    (phaseResult.map(f(event)), state)
//  }
//
//  override def initialState = phaseParser.initialState
//}
//
//case class MapParser[Event, State, In, Out](phaseParser: PhaseParser[Event, State, In], f: In => Out) extends PhaseParser[Event, State, Out] {
//
//  override def apply(event: Event, oldState: State): (PhaseResult[Out], State) = {
//    val (phaseResult, state) = phaseParser.apply(event, oldState)
//    (phaseResult.map(f), state)
//  }
//
//  override def initialState = phaseParser.initialState
//}
//
//
///**
//  * Phase terminating inner parser. If inner parser at least once got to the TerminalResult it will stay there forever
//  *
//  * @param inner - parser to be terminated
//  * @tparam Event - events to process
//  * @tparam State - inner state
//  * @tparam Out
//  */
//case class Terminate[Event, State, Out](inner: PhaseParser[Event, State, Out]) extends PhaseParser[Event, (PhaseResult[Out], State), Out] {
//
//  override def apply(event: Event, v2: (PhaseResult[Out], State)): (PhaseResult[Out], (PhaseResult[Out], State)) = {
//    val (phaseResult, state) = v2
//
//    phaseResult match {
//      case x: TerminalResult[Out] => x -> v2
//      case Stay =>
//        val (nextResult, nextState) = inner.apply(event, state)
//        nextResult -> (nextResult, nextState)
//    }
//  }
//
//  override def initialState = Stay -> inner.initialState
//}
//
//trait VectorStatefulParser[Event, State, Out] extends PhaseParser[Event, State, Out]{
//
//  override def size = 1
//}
//
//
//trait VectorOneRowPhaseParser[Event, +T] extends VectorPhaseParser[Event, Unit, T] {
//
//  override def apply(v1: Event, v2: Packed[Unit]): (PhaseResult[T], Unit) = Success(extract(v1)) -> ()
//
//  def extract(event: Event): T
//
//  override def initialState = ()
//
//  override def size: Int = 0
//}