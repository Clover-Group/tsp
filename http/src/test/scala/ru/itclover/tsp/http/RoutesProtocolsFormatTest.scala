package ru.itclover.tsp.http

import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.streaming.io.{NarrowDataUnfolding, WideDataFilling}
import spray.json.{JsBoolean, JsNumber, JsString, JsValue}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Also, this test uses Any values as test cases and asInstanseOf methods for type conversion.
@SuppressWarnings(
  Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf")
)
class RoutesProtocolsFormatTest extends AnyFlatSpec with Matchers with RoutesProtocols {

  case class TestClass(value: Int)

  "RoutesProtocols AnyRef format" should "correctly handle JSON" in {
    propertyFormat.write(1.asInstanceOf[AnyRef]) shouldBe JsNumber(1)
    propertyFormat.write(1L.asInstanceOf[AnyRef]) shouldBe JsNumber(1)
    propertyFormat.write(true.asInstanceOf[AnyRef]) shouldBe JsBoolean(true)
    propertyFormat.write("test") shouldBe JsString("test")
    propertyFormat.write(TestClass(42)) shouldBe JsString("TestClass(42)")
    propertyFormat.read(JsNumber(1.0)) shouldBe 1.asInstanceOf[AnyRef]
    propertyFormat.read(JsString("test")) shouldBe "test"
    propertyFormat.read(JsBoolean(true)) shouldBe true.asInstanceOf[AnyRef]
  }

  "RoutesProtocols Any format" should "correctly handle JSON" in {
    anyFormat.write(1) shouldBe JsNumber(1)
    anyFormat.write(1L) shouldBe JsNumber(1)
    anyFormat.write(true) shouldBe JsBoolean(true)
    anyFormat.write("test") shouldBe JsString("test")
    anyFormat.write(TestClass(42)) shouldBe JsString("TestClass(42)")
    anyFormat.read(JsNumber(1.0)) shouldBe 1
    anyFormat.read(JsString("test")) shouldBe "test"
    anyFormat.read(JsBoolean(true)) shouldBe true
  }

  "SDT formats" should "work" in {
    sdtFormat[Any, String, String]
      .write(NarrowDataUnfolding("key", "value", Map.empty[String, Long])) shouldBe a[JsValue]
    sdtFormat[Any, String, String].write(
      WideDataFilling[Any, String, String](Map.empty[String, Long], Some(0L))
    ) shouldBe a[
      JsValue
    ]
  }

}
