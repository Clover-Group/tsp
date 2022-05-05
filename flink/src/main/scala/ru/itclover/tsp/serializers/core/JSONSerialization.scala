package ru.itclover.tsp.serializers.core

import com.fasterxml.jackson.databind.node.ObjectNode

import java.nio.charset.Charset
import java.sql.Timestamp
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.{EventSchema, EventSchemaValue, FloatESValue, IntESValue, NewRowSchema, ObjectESValue, StringESValue}

/**
  * JSON Serialization for Redis
  */
class JSONSerialization extends Serialization[Array[Byte], Row] {

  /**
    * Method for deserialize from json string
    * @param input bytes array from json string
    * @return flink row
    */
  override def deserialize(input: Array[Byte], fieldsIdxMap: Map[Symbol, Int]): Row = {

    val inputData = new String(input)
    val jsonTree = new ObjectMapper().readTree(inputData)
    val row = new Row(fieldsIdxMap.size)
    val mapper = new ObjectMapper()

    fieldsIdxMap.foreach {
      case (elem, index) =>
        val rawValue = jsonTree.get(elem.name)
        val fieldValue = mapper.convertValue(rawValue, classOf[java.lang.Object])
        row.setField(index, fieldValue)
    }

    row

  }


  /**
    * Method for serialize to json string
    * @param output flink row
    * @param rowSchema schema from flink row
    * @return bytes array from json string
    */
  // Flink rows are untyped, so asInstanceOf
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.NonUnitStatements"))
  override def serialize(output: Row, eventSchema: EventSchema): Array[Byte] = {

    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()

    // TODO: Write JSON

    eventSchema match {
      case newRowSchema: NewRowSchema =>
        newRowSchema.data.foreach { case (k, v) =>
          putValueToObjectNode(k, v, root, output.getField(newRowSchema.fieldsIndices(Symbol(k))))
        }
    }

    def putValueToObjectNode(k: String,
                             v: EventSchemaValue,
                             root: ObjectNode,
                             value: Object): Unit = {
      v.`type` match {
        case "int8" => root.put(k, value.asInstanceOf[Byte])
        case "int16" => root.put(k, value.asInstanceOf[Short])
        case "int32" => root.put(k, value.asInstanceOf[Int])
        case "int64" => root.put(k, value.asInstanceOf[Long])
        case "float32" => root.put(k, value.asInstanceOf[Float])
        case "float64" => root.put(k, value.asInstanceOf[Double])
        case "boolean" => root.put(k, value.asInstanceOf[Boolean])
        case "string" => root.put(k, value.asInstanceOf[String])
        case "timestamp" => root.put(k, value.asInstanceOf[Timestamp].toString)
        case "object" =>
          val node: ObjectNode = mapper.createObjectNode()
          val data = value.asInstanceOf[Map[String, Any]]
          data.foreach { case (k, v) =>
            v match {
              case x: Byte => node.put(k, x)
              case x: Short => node.put(k, x)
              case x: Int => node.put(k, x)
              case x: Long => node.put(k, x)
              case x: Float => node.put(k, x)
              case x: Double => node.put(k, x)
              case x: Boolean => node.put(k, x)
              case x: String => node.put(k, x)
              case _ => node.put(k, v.toString)
            }
          }
          root.put(k, node)
      }
    }

    /*eventSchema match {
      case newRowSchema: NewRowSchema =>
        root.put(newRowSchema.unitIdField.name, output.getField(newRowSchema.unitIdInd).asInstanceOf[Int])
        root.put(newRowSchema.fromTsField.name, output.getField(newRowSchema.beginInd).asInstanceOf[Timestamp].toString)
        root.put(newRowSchema.toTsField.name, output.getField(newRowSchema.endInd).asInstanceOf[Timestamp].toString)
        root.put(newRowSchema.appIdFieldVal._1.name, output.getField(newRowSchema.appIdInd).asInstanceOf[Int])
        root.put(newRowSchema.patternIdField.name, output.getField(newRowSchema.patternIdInd).asInstanceOf[Int])
        root.put(newRowSchema.subunitIdField.name, output.getField(newRowSchema.subunitIdInd).asInstanceOf[Int])
        root.put(newRowSchema.incidentIdField.name, output.getField(newRowSchema.incidentIdInd).asInstanceOf[String])
        newRowSchema.context match {
          case Some(Context(field, data)) =>
            val node: ObjectNode = mapper.createObjectNode()
            data.foreach { case (k, v) =>
              node.put(k.name, v)
            }
            root.put(field.name, node)
          case None =>
        }
    }*/

    val jsonString = mapper.writeValueAsString(root)

    jsonString.getBytes(Charset.forName("UTF-8"))

  }
}
