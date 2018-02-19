package ru.sbt.windowing.eventtime

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.tweets.{Tweet, TweetsChampion}
import ru.sbt.utils.TweetsStreamer
import org.apache.flink.streaming.api.scala._


object FixedLatenessMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Application starting")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamer = new TweetsStreamer(env)
    val tweetsStream = streamer.buildStream("data/ordered_tweets.csv")


    tweetsStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Tweet](Time.seconds(10)) {
        override def extractTimestamp(tweet: Tweet): Long = tweet.tweetDateTime.toInstant.toEpochMilli
      })
      .map{ tweet => TweetsChampion(tweet.userId) }
      .keyBy("userId")
      .timeWindow(Time.minutes(1))
      .sum("tweetCount")
      .addSink(tweetsChampion => logger.info(s"$tweetsChampion"))

    env.execute()

    logger.info("Done")
  }
}
