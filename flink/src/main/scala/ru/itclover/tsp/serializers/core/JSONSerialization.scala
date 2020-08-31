package ru.itclover.tsp.serializers.core

import java.nio.charset.Charset
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.{EventSchema, NewRowSchema, RowSchema}

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
  override def serialize(output: Row, eventSchema: EventSchema): Array[Byte] = {

    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()

    eventSchema match {
      case rowSchema: RowSchema =>
        root.put(rowSchema.sourceIdField.name, output.getField(rowSchema.sourceIdInd).asInstanceOf[Int])
        root.put(rowSchema.fromTsField.name, output.getField(rowSchema.beginInd).asInstanceOf[Double])
        root.put(rowSchema.toTsField.name, output.getField(rowSchema.endInd).asInstanceOf[Double])
        root.put(rowSchema.appIdFieldVal._1.name, output.getField(rowSchema.appIdInd).asInstanceOf[Int])
        root.put(rowSchema.patternIdField.name, output.getField(rowSchema.patternIdInd).asInstanceOf[String])
        root.put(rowSchema.processingTsField.name, output.getField(rowSchema.processingTimeInd).asInstanceOf[Double])
        root.put(rowSchema.contextField.name, output.getField(rowSchema.contextInd).asInstanceOf[String])
      case newRowSchema: NewRowSchema =>
        root.put(newRowSchema.unitIdField.name, output.getField(newRowSchema.unitIdInd).asInstanceOf[Int])
        root.put(newRowSchema.fromTsField.name, output.getField(newRowSchema.beginInd).asInstanceOf[Timestamp].toString)
        root.put(newRowSchema.toTsField.name, output.getField(newRowSchema.endInd).asInstanceOf[Timestamp].toString)
        root.put(newRowSchema.appIdFieldVal._1.name, output.getField(newRowSchema.appIdInd).asInstanceOf[Int])
        root.put(newRowSchema.patternIdField.name, output.getField(newRowSchema.patternIdInd).asInstanceOf[String])
        root.put(newRowSchema.subunitIdField.name, output.getField(newRowSchema.subunitIdInd).asInstanceOf[String])
    }

    val jsonString = mapper.writeValueAsString(root)

    jsonString.getBytes(Charset.forName("UTF-8"))

  }
}
