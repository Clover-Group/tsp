package ru.itclover.streammachine

import java.sql.Timestamp
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import ru.itclover.streammachine.core.Aggregators
import ru.itclover.streammachine.core.NumericPhaseParser.SymbolNumberExtractor
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.{ClickhouseInput, StorageFormat}
import ru.itclover.streammachine.io.input.StorageFormat.StorageFormat
import ru.itclover.streammachine.transformers.{FlinkStateCodeMachineMapper, SparseRowsDataAccumulator}
import ru.itclover.streammachine.io.input.JDBCInputConfig
import ru.itclover.streammachine.io.input.{ClickhouseInput, StorageFormat}
import ru.itclover.streammachine.io.output.JDBCOutputConfig
import sun.reflect.generics.reflectiveObjects.NotImplementedException



object FlinkStream {

  val log = Logger("FlinkStream")
  val defaultFieldTimeout: Long = 60000L

  // TODO: Other configs
  def createPatternSearchStream(input: JDBCInputConfig, output: JDBCOutputConfig, phaseCode: String,
                                storageType: StorageFormat)
                               (implicit streamEnv: StreamExecutionEnvironment):
      (StreamExecutionEnvironment, DataStream[Aggregators.Segment]) = {
    val fieldsTypesInfo = ClickhouseInput.queryFieldsTypeInformation(input) match {
      case Right(typesInfo) => typesInfo
      case Left(err) => throw err
    }
    log.info(s"fieldsTypesInfo: `${fieldsTypesInfo.mkString(", ")}")
    val chInputFormat: JDBCInputFormat = ClickhouseInput.getInputFormat(input, fieldsTypesInfo.toArray)
    val fields = fieldsTypesInfo.map(_._1).map(Symbol(_))
    val fieldsIndexesMap = fields.zipWithIndex.toMap
    implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
      override def apply(v1: Row) = {
        v1.getField(fieldsIndexesMap(input.datetimeColname)).asInstanceOf[Timestamp]
      }
    }
    implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
      // TODO: Make it serializable
      override def extract(event: Row, symbol: Symbol): Double = {
        event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Double]
      }
    }

    val stream = streamEnv.createInput(chInputFormat).keyBy(
      input.partitionColnames.map(column => fieldsIndexesMap(column)):_*
    )

    val denseStream = storageType match {
      case StorageFormat.Narrow =>
        val defaultTimeouts = Seq.fill(fields.length)(defaultFieldTimeout)
        // TODO(1): (0, 1), Seq(1 -> 'a) params
        throw new NotImplementedException()
        val dataAccumulator = SparseRowsDataAccumulator(fields.zip(defaultTimeouts).toMap, (0, 1), Seq(1 -> 'a))(timeExtractor)
        stream.flatMap(dataAccumulator)

      case StorageFormat.WideAndDense => stream
    }

    val mapper = FlinkStateCodeMachineMapper(phaseCode, fieldsIndexesMap, SegmentResultsMapper[Row, Any],
      fieldsIndexesMap(input.datetimeColname))

    val resultStream = denseStream.flatMap(mapper)

    (streamEnv, resultStream)
  }

}
