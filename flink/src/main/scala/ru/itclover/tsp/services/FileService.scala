package ru.itclover.tsp.services

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.LocalDateTime

import scala.util.Random

object FileService {

  /**
    * Method for creating temp file
    * @return temp file path
    */
  def createTemporaryFile(): Path = {

    val currentTime = LocalDateTime.now().toString
    val randomInd = Random.nextInt(Integer.MAX_VALUE)

    Files.createTempFile(s"temp_${randomInd}_($currentTime)", ".temp")

  }

  /**
    * Method for converting input bytes to file
    * @param input bytes for convert
    * @return file with input bytes
    */
  def convertBytes(input: Array[Byte]): File = {
    val path = FileService.createTemporaryFile()

    val options = Set(
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    ).toSeq

    val fileChannel = FileChannel.open(path, options: _*)
    val buffer = ByteBuffer.wrap(input)

    fileChannel.write(buffer)
    fileChannel.close()

    path.toFile

  }

}
