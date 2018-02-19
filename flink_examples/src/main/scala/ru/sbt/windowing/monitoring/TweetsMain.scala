package ru.sbt.windowing.monitoring

import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.tweets.{Tweet, TweetsChampion}
import ru.sbt.utils.TweetsStreamer


object TweetsMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Application starting")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamer = new TweetsStreamer(env)
    val tweetsStream = streamer.buildStream("data/tweets.csv")


    val socketTweets: DataStream[Tweet] = env.socketTextStream("127.0.0.1", 9999)
      .map( _ =>
        {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          Tweet("", dateFormat.parse("2017-01-01 00:00:00"), Seq.empty[String])
        })
      .filter(_ => false)


    socketTweets.union(tweetsStream)
      .assignTimestampsAndWatermarks(new TweetTimestampsAndWatermarks())
      .map(tweet => TweetsChampion(tweet.userId))
      .keyBy("userId")
      .timeWindow(Time.hours(1))
      .sum("tweetCount")
      .keyBy("userId")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .maxBy("tweetCount")
      .addSink(tweetsChampion => logger.info(s"$tweetsChampion"))

    env.execute()

    logger.info("Done")
  }
}
