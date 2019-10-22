//package ru.itclover.tsp.deserializers
//
//import java.io.{File,FileOutputStream}
//import org.apache.flink.api.common.serialization.DeserializationSchema
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.TypeExtractor
//import ru.itclover.tsp.io.input.InputData
//import ru.itclover.tsp.utils.ArrowOps
//
///**
//* Deserializer for Arrow file from Kafka
//  */
//object KafkaArrowDeserializer extends DeserializationSchema[InputData]{
//
//  override def isEndOfStream(t: InputData): Boolean = true
//
//  override def deserialize(bytes: Array[Byte]): InputData = {
//
//    val tempFile = File.createTempFile("test", "tmp")
//    val outputStream = new FileOutputStream(tempFile)
//    outputStream.write(bytes)
//
//    val test = ArrowOps.readFromFile(tempFile)
//
//    println(s"TEST data: $test")
//
//    InputData(1)
//
//  }
//
//  override def getProducedType: TypeInformation[InputData] = TypeExtractor.getForClass(classOf[InputData])
//
//}
