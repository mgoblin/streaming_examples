package ru.sbt.unit_testing.transform

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TransformDataMain extends LazyLogging {

  val reverse: String => String = _.reverse

  def main(args: Array[String]): Unit = {
    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketTextStream = env.socketTextStream("127.0.0.1", 9999)

    // Map
    val mappedStream = socketTextStream.map( reverse )
    mappedStream.addSink(s => logger.info(s"Map: $s"))

    env.execute("Simple data transformations")

    logger.info("Done")
  }
}
