package ru.itclover.streammachine.serializers

import java.io.IOException
import org.apache.flink.types.Row
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.concurrent.duration._
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither


case class RowAvroSerializer(fieldsIndexesMap: Map[Symbol, Int], schema: Schema)
                            (implicit ty: TypeInformation[Row]) extends SerializationSchema[Row] {

  @transient
  private var dtw: GenericDatumWriter[GenericData.Record] = getOrCreateWriter(schema)

  override def serialize(element: Row): Array[Byte] = {
    ensureInitialized()

    val record = new GenericData.Record(schema)
    fieldsIndexesMap foreach { fieldAndInd =>
      record.put(fieldAndInd._1.toString.tail, element.getField(fieldAndInd._2))
    }

    val arrOutStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(arrOutStream, null)
    try {
      arrOutStream.reset()
      dtw.write(record, encoder)
      encoder.flush()
      arrOutStream.toByteArray
    }
    catch {
      case ex: IOException => throw new RuntimeException(s"Fail to serialize Row", ex)
    }
  }

  private def ensureInitialized(): Unit = {
    if (dtw == null) {
      dtw = getOrCreateWriter(schema)
    }
  }

  private def getOrCreateWriter(schema: Schema) = {
    if (dtw == null) {
      dtw = new GenericDatumWriter[GenericData.Record](schema)
    }
    dtw
  }
}
