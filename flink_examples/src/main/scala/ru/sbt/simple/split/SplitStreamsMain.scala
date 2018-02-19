package ru.sbt.simple.split

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._

object SplitStreamsMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)

    val socketTextStream = env.socketTextStream("localhost", 9999)
      .split(s =>
        s.split(' ')
          .take(1)
          .map(s => if (s == "hello" || s == "hi") s else "other"))

    socketTextStream
      .select("hello")
      .addSink(s => logger.info(s"hello stream: $s"))

    socketTextStream
      .select("hi")
      .addSink(s => logger.info(s"hi stream: $s"))

    socketTextStream
      .select("other")
      .addSink(s => logger.info(s"other stream: $s"))

    env.execute("Split streams")

    logger.info("Done")
  }
}
