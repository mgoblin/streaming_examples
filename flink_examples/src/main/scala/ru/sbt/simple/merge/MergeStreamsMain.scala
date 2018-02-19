package ru.sbt.simple.merge

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._

object MergeStreamsMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)

    val socketTextStream9998 = env.socketTextStream("localhost", 9998)
    val socketTextStream9999 = env.socketTextStream("localhost", 9999)

    socketTextStream9998.addSink(s => logger.info(s"Stream 9998: $s"))
    socketTextStream9999.addSink(s => logger.info(s"Stream 9999: $s"))

    val marked9998 = socketTextStream9998.map(s => s"I am 9998 message: $s")
    val marked9999 = socketTextStream9999.map(s => s"I am 9999 message: $s")

    marked9998
      .union(marked9999)
      .addSink(s => logger.info(s"Union 9998&9999: $s"))

    env.execute("Merge streams")

    logger.info("Done")
  }
}
