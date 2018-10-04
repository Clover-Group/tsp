## Pattern type

> Note: {% include types-note.md %}

```scala
case class RawPattern(id: String, sourceCode: String, payload: Map[String, String] = Map.empty,
                      forwardedFields: Seq[Symbol] = Seq.empty) extends Serializable
```
