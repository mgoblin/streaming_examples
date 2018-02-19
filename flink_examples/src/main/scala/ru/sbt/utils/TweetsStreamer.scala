package ru.sbt.utils

import java.text.SimpleDateFormat

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import ru.sbt.tweets.Tweet


class TweetsStreamer(streamEnv: StreamExecutionEnvironment) extends CSVStreamer(streamEnv) {

  def  buildStream(fileName: String): DataStream[Tweet] = {

    val csvParams = CSVParams(
      "twitsTable",
      path = fileName,
      fieldNames = Array("userId", "tweetDate", "tags"),
      fieldTypes = Array(Types.STRING, Types.STRING, Types.STRING),
      fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER,
      rowDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER,
      quoteCharacter = null,
      ignoreFirstLine = false,
      ignoreComments = null,
      lenient = false
    )

    buildStream(csvParams)
      .map { row =>
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        Tweet(
          userId = row.getField(0).asInstanceOf[String],
          tweetDateTime = dateFormat.parse(row.getField(1).asInstanceOf[String]),
          tags = row.getField(2).asInstanceOf[String].split("#").filter(_.trim.nonEmpty)
        )
      }
  }
}
