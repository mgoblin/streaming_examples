package ru.sbt

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.streaming.api.scala._

object PeriodicWatermarksMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting Flink watermark example")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setAutoWatermarkInterval(1000)

    val stringStream = env.socketTextStream("127.0.0.1", 9999)
      .name("Socket reader")
      .assignTimestampsAndWatermarks(new LoggedIngestionWatermarkExtractor[String]())
      .map(s => (s, s.length))
      .global
      .timeWindowAll(Time.seconds(10))
      .sum("_2")
      .name("Message and length")

    stringStream.addSink(s => logger.info(s"Sum is '${s._2}'"))

    env.execute("Flink watermark example")

    logger.info("Done")
  }
}
