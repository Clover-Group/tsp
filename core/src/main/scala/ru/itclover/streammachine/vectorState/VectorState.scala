//package ru.itclover.streammachine.vectorState
//
//import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}
//import ru.itclover.streammachine.core.PhaseParser.And
//import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
//import ru.itclover.streammachine.vectorState.PackedPhaseParser.Packed
//
//
//class VectorState {
//
//}
//
//trait Packable[State] {
//  def build(vector: Vector[Any], i: Int): Packed[State] = (vector, i)
//
//  def unpack(packed: Packed[State]): State
//
//  def size: Int
//}
//
//
//trait ParserPacker[Parser <: ({type L[Event, State, Out] = PhaseParser[Event, State, Out]})#L]{
//  def pack(parser: Parser) = PackedPhaseParser[Parser]()
//}
//
//val oneRowParserPacker = {
//  pack = >
//}
//
//  val singleStatePacker = {
//
//}
//
//  val andParserPacker = {
//
//}
//
//
//  pack[Parser: Packer]( packer.pack)
//
//
//object ParserPacker{
//
//}
//
//
//trait SimpleParser {
//  size = 1
//}
//
//trait ComposableParser {
//  size = Left.size + ri
//
//}
//
//object PackedPhaseParser {
//  type Packed[State] = (Vector[Any], Int)
//
//  type K = ({type L[Event, State, Out] = PhaseParser[Event, State, Out]})#L
//
//  val k: K
//
//
//  def pack[Event, State, Out](phaseParser: PhaseParser[Event, State, Out]): PackedPhaseParser[Event, State, Out]
//}
//
//trait WithIndex {
//  def index: Int
//
//  def size: Int
//}
//
//class PackedPhaseParser[Event, State, Out] extends PhaseParser[Event, Packed[State], Out] with WithIndex {
//
//  override val index = 1
//
//
//  override def apply(v1: Event, v2: Packed[State]) = {
//    val state = unpack(v2)(index, size)
//    super.apply(v1, v2)
//  }
//
//  override def initialState = ???
//}
//
//case class PackedAndParser[Event, LState: Packable, RState: Packable, LOut, ROut]
//(
//  leftParser: PhaseParser[Event, LState, LOut],
//  rightParser: PhaseParser[Event, RState, ROut]
//)
//  extends PhaseParser[Event, LState And RState, LOut And ROut] {
//
//  override def apply(event: Event, packedState: Packed[LState And RState]): (PhaseResult[LOut And ROut], LState And RState) = {
//    val (leftState, rightState) = state
//
//    val currentIndex = packedState._2
//    val lPackable = packable[LState]
//
//    leftState: Packed[LState]
//    = lPackable.build(packedState._1, currentIndex + 0)
//    rightState: Packed[RState]
//    = rPackable.build(packedState._1, currentIndex + lPackable.size)
//
//
//    val (leftResult, newLeftState) = pack(leftParser)(event, leftState)
//    val (rightResult, newRightState) = rightParser(event, rightState)
//    val newState = newLeftState -> newRightState
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