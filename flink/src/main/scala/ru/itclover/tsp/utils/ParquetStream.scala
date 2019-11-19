package ru.itclover.tsp.utils

import java.io.ByteArrayInputStream

import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile, SeekableInputStream}
import ru.itclover.tsp.utils.ParquetStream.SeekableByteArrayInputStream

/**
  * Adapter class to read data in parquet format from byte arrays.
  *
  * @param bytes - input byte array. Must not change after creation.
  * @author Bulat Fattakhov
  */
class ParquetStream(bytes: Array[Byte]) extends InputFile {

  override def getLength: Long = bytes.length

  override def newStream: SeekableInputStream = new DelegatingSeekableInputStream(
    new SeekableByteArrayInputStream(bytes)
  ) {

    override def seek(newPos: Long): Unit = {
      this.getStream.asInstanceOf[SeekableByteArrayInputStream].setPos(newPos.intValue())
    }

    override def getPos: Long =
      this.getStream.asInstanceOf[SeekableByteArrayInputStream].getPos.toLong
  }
}

object ParquetStream {

  class SeekableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {

    def setPos(pos: Int): Unit = this.pos = pos

    def getPos: Int = this.pos
  }
}