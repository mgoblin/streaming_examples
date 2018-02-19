package ru.sbt

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class LoggedIngestionPunctuatedWatermarks extends AssignerWithPunctuatedWatermarks[String] with LazyLogging {

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    val timestamp = System.currentTimeMillis()
    logger.info(s"get element '$element' watermark $timestamp")
    timestamp
  }

  override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
    val dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

    try {
      val dateTime = LocalDateTime.parse(lastElement, dateTimeFormatter)
      val millis = dateTime.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
      val watermark = new Watermark(millis)

      logger.info(s"get punctuated watermark $watermark")

      watermark
    } catch {
      case ex: Throwable =>
        logger.info(s"$lastElement is not datetime dd-MM-yyyy HH:ss.")
        null
    }
  }
}
