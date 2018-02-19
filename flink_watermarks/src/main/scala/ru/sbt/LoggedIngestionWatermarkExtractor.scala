package ru.sbt

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor

class LoggedIngestionWatermarkExtractor[T] extends IngestionTimeExtractor[T] with LazyLogging {

  override def getCurrentWatermark = {
    val timestamp = super.getCurrentWatermark
    logger.info(s"get current watermark $timestamp")
    timestamp
  }

  override def extractTimestamp(element: T, previousElementTimestamp: Long) = {
    val timestamp = super.extractTimestamp(element, previousElementTimestamp)
    logger.info(s"extract timestamp $timestamp from $element")
    timestamp
  }
}
