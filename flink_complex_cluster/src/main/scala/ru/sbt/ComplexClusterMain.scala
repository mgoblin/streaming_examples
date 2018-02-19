package ru.sbt

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object ComplexClusterMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Complex flink flow starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream = env
      .socketTextStream("127.0.0.1", 9999)
      .name("Socket reader")

    socketStream
      .flatMap( s => s.split(" "))
      .setParallelism(4)
      .name("Split string by words")
      .addSink(s => logger.info(s))
      .name("Save to log")

    env.execute("Complex Flink flow")

    logger.info("Done")
  }
}
