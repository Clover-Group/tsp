package ru.itclover.tsp.utils
import java.lang.Exception
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.utils.ErrorsADT._

class ErrorsADTTest extends FlatSpec with Matchers {
  "Request errors" should "inform the user correctly" in {
    InvalidRequest("test").errorCode shouldBe 4010
    InvalidRequest("test", 4011).errorCode shouldBe 4011
    InvalidPatternsCode(Seq("one", "two", "three")).errorCode shouldBe 4020
    InvalidPatternsCode(Seq("one", "two", "three"), 4021).errorCode shouldBe 4021
    InvalidPatternsCode(Seq("one", "two", "three")).error shouldBe "one\ntwo\nthree"
    SourceUnavailable("test").errorCode shouldBe 4030
    SourceUnavailable("test", 4031).errorCode shouldBe 4031
    SinkUnavailable("test").errorCode shouldBe 4040
    SinkUnavailable("test", 4041).errorCode shouldBe 4041
    GenericConfigError(new Exception()).errorCode shouldBe 4000
    GenericConfigError(new Exception(), 4001).errorCode shouldBe 4001
    GenericConfigError(new Exception()).error shouldBe "java.lang.Exception"
    GenericConfigError(new Exception("message")).error shouldBe "message"
    GenericRuntimeErr(new Exception()).errorCode shouldBe 5000
    GenericRuntimeErr(new Exception(), 5001).errorCode shouldBe 5001
    GenericRuntimeErr(new Exception()).error shouldBe "java.lang.Exception"
    GenericRuntimeErr(new Exception("message")).error shouldBe "message"
  }
}
