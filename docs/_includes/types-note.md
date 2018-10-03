Types are just Scala classes that translates on JSON directly,
for example, for class `case class Buzz(x: Int, y: Map[String, String])`
valid JSON representation could be: `{ "x": 120, "y": {"a": 1} }`.
Also `Option` types represent optional parameters.