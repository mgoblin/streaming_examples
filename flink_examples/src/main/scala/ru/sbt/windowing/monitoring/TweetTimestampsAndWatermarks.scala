package ru.sbt.windowing.monitoring

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import ru.sbt.tweets.Tweet

import scala.math._

class TweetTimestampsAndWatermarks extends AssignerWithPeriodicWatermarks[Tweet] with LazyLogging {

  val maxTimeLag = 3500L

  var currentMaxTimestamp: Long = 0

  var lagMultyplier = 1

  override def extractTimestamp(element: Tweet, previousElementTimestamp: Long) = {
    val timestamp = element.tweetDateTime.toInstant.toEpochMilli
    logger.info(s"Timestamp for $element is $timestamp")
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    logger.info(s"currentMaxTimestamp is $currentMaxTimestamp for $element")
    timestamp
  }

  override def getCurrentWatermark = {
    val currentTime = System.currentTimeMillis
    val watermarkTime =
      if (currentTime <= currentMaxTimestamp) {
        logger.info(s"Send current watermark timestamp $currentTime")
        currentTime
      } else {
        val timestamp = currentMaxTimestamp

        currentMaxTimestamp += lagMultyplier * maxTimeLag
        lagMultyplier *= 10

        logger.info(s"Send late watermark timestamp $timestamp")
        timestamp
      }

    val watermark = new Watermark(watermarkTime)
    watermark
  }
}
