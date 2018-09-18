# Architectural overview

### Core components of TSP (`core` project)

- `Pattern[Event, State, Result]` - logic for transforming incoming 
event and previous `State` to some `PatternResult`, 
in other words is function of type: <br>
`(Event, State) => (Seq[PatternResult], State)`.
- `State` - arbitrary values that can be used inside of patterns,
management of this state and patterns happens in `PatternMapper`
- `PatternResult[T]` - one of the next values
(emits from each call of the `Pattern`):
    - `Success(value: T)` - signals about successfully found pattern (_terminal_)
    - `Failure(msg: String)` - pattern was not found due to `msg` (_terminal_)
    - `Stay` - accumulating some state, for example gather
    values to compute `avg` phase (_non-terminal_)
- `PatternMapper` - run it all together and provide simple interface for
further usage in Flink-connector, for example.


Let's look at simple example of Abs phase with minor simplifications 
(`NumericPattern` is only alias for `Pattern[Double, _, _]`):<br>
```scala
case class AbsPattern[Event, State](numeric: NumericPattern[Event, State])
     extends NumericPattern[Event, State] {

    override def apply(event: Event, state: State) = {
      val (innerResult, newState) = numeric(event, state)
      (innerResult match {
        case Success(x) => Success(Math.abs(x))
        case x => x
      }) -> newState
    }

    override def initialState = numeric.initialState
}
```

Note, that patterns can be joined together by hierarchical composition
(numeric argument in class AbsPhase).
<br>

More complex example:
```scala
case class EitherPattern[Event, LState, RState, LOut, ROut]
(
  leftPattern: PhaseParser[Event, LState, LOut],
  rightPattern: PhaseParser[Event, RState, ROut]
)
  extends Pattern[Event, (LState, RState), LOut Or ROut] {

  override def apply(event: Event, state: (LState, RState)): (PhaseResult[LOut Or ROut], (LState, RState)) = {
    val (leftState, rightState) = state

    val (leftResult, newLeftState) = leftPattern(event, leftState)
    val (rightResult, newRightState) = rightPattern(event, rightState)
    val newState = newLeftState -> newRightState
    ((leftResult, rightResult) match {
      case (Success(leftOut), _) => Success(Left(leftOut))
      case (_, Success(rightOut)) => Success(Right(rightOut))
      case (Stay, _) => Stay
      case (_, Stay) => Stay
      case (Failure(msg1), Failure(msg2)) => Failure(s"Or Failed: 1) $msg1 2) $msg2")
    }) -> newState
  }

  override def initialState = (leftPattern.initialState, rightPattern.initialState)
}
```
Here we combine left and right phases together in Either style:
- if either of them becomes `Success` - the result will be successful
- if either of them `Failure` - the result will be failure
- otherwise `Stay`