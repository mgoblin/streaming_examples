package ru.sbt

import com.typesafe.scalalogging.LazyLogging

import org.apache.flink.streaming.api.scala._

object ClusterMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Flink job starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .socketTextStream("localhost", 9999)
      .map(s => s"Received socket data $s")
      .addSink(s => logger.info(s))

    env.execute("Flink socket for cluster with map")

    logger.info("Flink job done")
  }
}
