# Writing patterns
Pattern is a hierarchical set of phases combined with special phases, like
`x andThen y` (first then second) or `x togetherWith y` (both in the same time).
To include stay phases in result wrap all expression in `ToSegments()`


### Combining phases
- All methods in [@CombiningPhasesSyntax](flink/src/main/scala/ru/itclover/streammachine/phases/CombiningPhases.scala) class.

### Numeric phases
- All methods in [@NumericPhasesSyntax](flink/src/main/scala/ru/itclover/streammachine/phases/NumericPhases.scala) class.

### TimePhases
- All methods in [@TimePhasesSyntax](flink/src/main/scala/ru/itclover/streammachine/phases/TimePhases.scala) class.

### Aggregation phases
- All methods in [@AggregatorPhasesSyntax](flink/src/main/scala/ru/itclover/streammachine/aggregators/AggregatorPhases.scala) class.

### Boolean phases
- All methods in [@BooleanPhasesSyntax](flink/src/main/scala/ru/itclover/streammachine/phases/BooleanPhases.scala) class.


## Examples
- Speed and pomp in bounds: `Assert('speed.field > 10) and Assert('pomp.field < 20)`

- Derivation of 5 sec avg speed > 0 and after that avg pump > 0
```scala
Assert(derivation(avg('speed.field, 5.seconds)) >= 0.0) andThen
  Assert(avg('pump.field, 3.seconds) > 0)
```