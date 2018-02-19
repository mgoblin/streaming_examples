package ru.sbt.utils

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

class CSVStreamer(val streamEnv: StreamExecutionEnvironment) {
  protected def buildStream(csvParams: CSVParams): DataStream[Row] = {
    val tableEnvironment = TableEnvironment.getTableEnvironment(streamEnv)

    val tableSource = new CsvTableSource(
      path = csvParams.path,
      fieldNames = csvParams.fieldNames,
      fieldTypes = csvParams.fieldTypes,
      fieldDelim = csvParams.fieldDelim,
      rowDelim = csvParams.rowDelim,
      quoteCharacter = csvParams.quoteCharacter,
      ignoreFirstLine = csvParams.ignoreFirstLine,
      ignoreComments = csvParams.ignoreComments,
      lenient = csvParams.lenient
    )

    tableEnvironment.registerTableSource(csvParams.tableName, tableSource)

    val table = tableEnvironment.scan(csvParams.tableName).select("*")
    table.toAppendStream[Row].startNewChain()
  }
}
