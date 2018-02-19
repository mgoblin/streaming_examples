package ru.sbt.windowing.monitoring

import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.tweets.{Tweet, TweetsChampion}
import ru.sbt.utils.TweetsStreamer

object TweetsNaiveMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Application starting")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)

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
      .map(tweet => TweetsChampion(tweet.userId))
      .keyBy("userId")
      .timeWindow(Time.seconds(10))
      .sum("tweetCount")
      .addSink(tweet => logger.info(s"$tweet"))

    env.execute()

    logger.info("Done")
  }
}
