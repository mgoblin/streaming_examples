package ru.sbt.simple.aggregate.reduce

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object AggregateDataMain extends LazyLogging {
  def main(args: Array[String]): Unit = {

    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)

    val socketTextStream = env.socketTextStream("localhost", 9999)

    val keyedStream: KeyedStream[String, String] = socketTextStream.keyBy(s => s)

    keyedStream.addSink(s => logger.info(s"KeyBy: $s"))

    socketTextStream
      .keyBy(s => s)
      .reduce((s1, s2) => s1 + s2)
      .addSink(s => logger.info(s"Reduce: $s"))


    socketTextStream
      .keyBy(s => s)
      .fold("Value chain:", new FoldFunction[String, String] {
        override def fold(accumulator: String, value: String) = {
          accumulator + " --> " + value
        }
      })
      .addSink(s => logger.info(s"Fold: $s"))

    env.execute("Aggregate data")

    logger.info("Done")

  }
}