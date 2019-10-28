package ru.itclover.tsp.services

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.{Matchers, WordSpec}

class ArrowServiceTest extends WordSpec with Matchers {

  val testPaths = List(
    "flink/src/test/resources/arrow/aaa2",
    "flink/src/test/resources/arrow/df_billion"
  )

  "ArrowService" should {

    "retrieve schema and reader from file" in {

      testPaths.foreach(path => {

        val schemaAndReader = ArrowService.retrieveSchemaAndReader(new File(path))

        schemaAndReader._1.getClass shouldBe classOf[Schema]
        schemaAndReader._2.getClass shouldBe classOf[ArrowFileReader]
        schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

      })

    }

    "read data from file" in {

      testPaths.foreach(path => {

        val start = System.nanoTime()

        val schemaAndReader = ArrowService.retrieveSchemaAndReader(new File(path))

        val result = ArrowService.convertData(schemaAndReader)

        val end = System.nanoTime()
        val elapsedTime = TimeUnit.SECONDS.convert(end - start, TimeUnit.NANOSECONDS)
        println(s"Execution time for file $path : ${end - start} nanoseconds, $elapsedTime seconds")

        result.nonEmpty shouldBe true

      })

    }

  }

}
