package ru.itclover.tsp.io.output

import java.io.ByteArrayOutputStream

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.formats.avro.AvroOutputFormat
import org.apache.flink.types.Row
import org.codehaus.jackson.map.ObjectMapper

trait OutputConf[Event] {
  def forwardedFieldsIds: Seq[Symbol]

  def getOutputFormat: OutputFormat[Event]

  def parallelism: Option[Int]

  def rowSchema: RowSchema
}

/**
  * Sink for anything that support JDBC connection
  * @param rowSchema schema of writing rows, __will be replaced soon__
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param batchInterval batch size for writing found incidents
  * @param parallelism num of parallel task to write data
  */
case class JDBCOutputConf(
  tableName: String,
  rowSchema: RowSchema,
  jdbcUrl: String,
  driverName: String,
  password: Option[String] = None,
  batchInterval: Option[Int] = None,
  userName: Option[String] = None,
  parallelism: Option[Int] = Some(1)
) extends OutputConf[Row] {
  override def getOutputFormat = JDBCOutput.getOutputFormat(this)

  override def forwardedFieldsIds = rowSchema.forwardedFields
}

///**
//  * "Empty" sink (for example, used if one need only to report timings)
//  */
//case class EmptyOutputConf() extends OutputConf[Row] {
//  override def forwardedFieldsIds: Seq[Symbol] = Seq()
//  override def getOutputFormat: OutputFormat[Row] = ???
//  override def parallelism: Option[Int] = Some(1)
//}

case class KafkaOutputConf(
  broker: String,
  topic: String,
  serializer: String,
  rowSchema: RowSchema,
  parallelism: Option[Int] = Some(1)
) extends OutputConf[Row] {
  override def forwardedFieldsIds: Seq[Symbol] = rowSchema.forwardedFields

  override def getOutputFormat: OutputFormat[Row] = new AvroOutputFormat(classOf[Row]) // actually not needed

  def jsonSerializer: SerializationSchema[Row] = (element: Row) => {
    val out = new ByteArrayOutputStream
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put(rowSchema.sourceIdField.name, element.getField(rowSchema.sourceIdInd).asInstanceOf[Int])
    root.put(rowSchema.fromTsField.name, element.getField(rowSchema.beginInd).asInstanceOf[Double])
    root.put(rowSchema.toTsField.name, element.getField(rowSchema.endInd).asInstanceOf[Double])
    root.put(rowSchema.appIdFieldVal._1.name, element.getField(rowSchema.appIdInd).asInstanceOf[Int])
    root.put(rowSchema.patternIdField.name, element.getField(rowSchema.patternIdInd).asInstanceOf[String])
    root.put(rowSchema.processingTsField.name, element.getField(rowSchema.processingTimeInd).asInstanceOf[Double])
    root.put(rowSchema.contextField.name, element.getField(rowSchema.contextInd).asInstanceOf[String])
    out.write(mapper.writeValueAsBytes(root))
    out.close()
    out.toByteArray
  }

}
