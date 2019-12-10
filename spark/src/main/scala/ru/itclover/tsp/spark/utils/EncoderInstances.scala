//package ru.itclover.tsp.spark.utils
//
//import org.apache.spark.sql.{Encoder, Encoders, Row}
//import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
//import ru.itclover.tsp.core.Incident
//import org.apache.spark.sql.catalyst.ScalaReflection
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//
//import scala.reflect.ClassTag

//object EncoderInstances {
//  implicit val anyEncoder: ExpressionEncoder[Any] = Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]]
//
//  implicit val incidentEncoder: ExpressionEncoder[Incident] = ExpressionEncoder()
//
//  implicit val sinkRowEncoder: Encoder[Row] = new Encoder[Row] {
//    override def schema: StructType = ScalaReflection.schemaFor[Row].dataType.asInstanceOf[StructType]
//
//    override def clsTag: ClassTag[Row] = ClassTag(classOf[Row])
//  }
//
//  implicit def rowWithIdxEncoder(rowEncoder: Encoder[Row]): ExpressionEncoder[RowWithIdx] =
//    Encoders.tuple[Row, Long](rowEncoder, Encoders.scalaLong).asInstanceOf[ExpressionEncoder[RowWithIdx]]
//
////  implicit def rowWithIdxEncoder(rowEncoder: Encoder[Row]): ExpressionEncoder[RowWithIdx] = new Encoder[RowWithIdx] {
////    override def schema: StructType = new StructType(
////      new StructField("idx", DataTypes.LongType) +: rowEncoder.schema.fields
////    )
////
////    override def clsTag: ClassTag[RowWithIdx] = ClassTag(classOf[RowWithIdx])
////  }
//}
