package ru.itclover.tsp.serializers.utils

import org.apache.flink.types.Row

import scala.collection.mutable

object SerializationUtils {

  def combineRows(input: mutable.ListBuffer[Row]): Row = {

    val size = input.head.getArity * input.size
    val row = new Row(size)
    var counter = 0

    input.foreach(rowInput => {

      val arity = rowInput.getArity

      (0 until arity).foreach(i => {

        row.setField(counter, rowInput.getField(i))
        counter += 1

      })

    })

    row

  }

}
