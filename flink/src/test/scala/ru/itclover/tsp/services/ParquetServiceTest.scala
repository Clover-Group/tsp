package ru.itclover.tsp.services

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType
import org.scalatest.{Matchers, WordSpec}

class ParquetServiceTest extends WordSpec with Matchers {

  val testPaths = List(
    "flink/src/test/resources/parquet/df_billion.parquet",
    "flink/src/test/resources/parquet/df_billion_brotli.parquet",
    "flink/src/test/resources/parquet/df_billion_gzip.parquet",
    "flink/src/test/resources/parquet/df_billion_snappy.parquet"
  )

  "ParquetService" should {

    "retrieve schema and reader from file" in {

      testPaths.foreach(path => {

        val schemaAndReader = ParquetService.retrieveSchemaAndReader(new File(path))

        schemaAndReader._1.getClass shouldBe classOf[MessageType]
        schemaAndReader._2.getClass shouldBe classOf[ParquetFileReader]

      })

    }

    "read data from file" in {

      testPaths.foreach(path => {

        val start = System.nanoTime()

        val schemaAndReader = ParquetService.retrieveSchemaAndReader(new File(path))

        val result = ParquetService.convertData(schemaAndReader)

        val end = System.nanoTime()
        val elapsedTime = TimeUnit.SECONDS.convert(end - start, TimeUnit.NANOSECONDS)
        println(s"Execution time for file $path : ${(end - start) / 1000} milliseconds, $elapsedTime seconds")

        result.nonEmpty shouldBe true

      })

    }

  }

}
