package ru.itclover.tsp.core
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor}

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S <: PState[T, S]](key: EKey)(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePatternLike[Event, T] { e =>

  override def idxExtractor: IdxExtractor[Event] = implicitly
  private val func: Event => T = extract.apply(key)(decoder) // curried function
  override val f: Event => Result[T] = e => Result.succ(func(e))

  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)
}
