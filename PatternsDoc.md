# Writing patterns
Pattern is a hierarchical set of phases combined with special ones, like
`x andThen y` (first then second) or `x togetherWith y` (both in the same time),
named combining phases.


### Combining phases
- All methods in [@CombiningPhasesSyntax](core/src/main/scala/ru/itclover/streammachine/phases/CombiningPhases.scala) class.

### Numeric phases
- All methods in [@NumericPhasesSyntax](core/src/main/scala/ru/itclover/streammachine/phases/NumericPhases.scala) class.

### TimePhases
- All methods in [@TimePhasesSyntax](core/src/main/scala/ru/itclover/streammachine/phases/TimePhases.scala) class.

### Aggregation phases
- All methods in [@AggregatorPhasesSyntax](core/src/main/scala/ru/itclover/streammachine/aggregators/AggregatorPhases.scala) class.
- To include stay phases in result wrap all expression in `ToSegments()`

### Boolean phases
- All methods in [@BooleanPhasesSyntax](core/src/main/scala/ru/itclover/streammachine/phases/BooleanPhases.scala) class.


## Examples
- Speed and pomp in bounds: `Assert('speed.field > 10) and Assert('pomp.field < 20)`

- Values in bounds for at least 7200 seconds, at max 72500 seconds:
    `ToSegments(Assert('PosKM.field === 0.0 and 'SpeedEngine.field != 0.0).timed(TimeInterval(1000.seconds, 7300.second)))`

- Derivation of 5 sec avg speed > 0 and after that avg pump > 0
```scala
Assert(derivation(avg('speed.as[Float], 5.seconds)) >= 0.0) andThen
  Assert(avg('pump.as[Float], 3.seconds) > 0)
```