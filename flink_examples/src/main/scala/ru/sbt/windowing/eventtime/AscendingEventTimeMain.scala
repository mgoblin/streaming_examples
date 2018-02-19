package ru.sbt.windowing.eventtime

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.tweets.TweetsChampion
import ru.sbt.utils.TweetsStreamer

object AscendingEventTimeMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Application starting")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamer = new TweetsStreamer(env)
    val tweetsStream = streamer.buildStream("data/ordered_tweets.csv")


    tweetsStream
      .assignAscendingTimestamps(_.tweetDateTime.toInstant.toEpochMilli)
      .map ( tweet => {
        logger.info(s"Tweet is $tweet")
        TweetsChampion(tweet.userId)
      })
      .keyBy("userId")
      .timeWindow(Time.minutes(1))
      .sum("tweetCount")
      .addSink(tweetsChampion => logger.info(s"$tweetsChampion"))

    env.execute()

    logger.info("Done")
  }
}
