package ru.itclover.tsp.utils

import java.io.File

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}

import scala.collection.mutable.ListBuffer

case class TempSchema(name: String, age: Int)

object ParquetOps {

  def writeToFile(file: File, data: List[TempSchema]): Unit = {

    val filePath = s"file://${file.getAbsolutePath}"
    ParquetWriter.write[TempSchema](filePath, data)

  }

  def readFromFile(file: File): List[TempSchema] = {

    val filePath = s"file://${file.getAbsolutePath}"

    val result = new ListBuffer[TempSchema]()
    val parquetIterable = ParquetReader.read[TempSchema](filePath)

    try{
      parquetIterable.foreach(elem => result += elem)
    }finally parquetIterable.close()

    result.toList

  }

}
