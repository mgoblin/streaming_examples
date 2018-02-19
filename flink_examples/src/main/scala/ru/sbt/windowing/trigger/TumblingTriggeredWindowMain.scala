package ru.sbt.windowing.trigger

import com.typesafe.scalalogging.LazyLogging

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ru.sbt.person.PersonStream

object TumblingTriggeredWindowMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting windowing triggered aggregation application")

    val host = "localhost"
    val port = 9999

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 2)
    val socketTextStream = env.socketTextStream(host, port)

    // В 20-секундном окне ожидать поступления двух персон с одинаковой фамилией. Суммировать возраст таких персон
    val personReduceStream = PersonStream(socketTextStream)
      .keyBy("lastName")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .trigger(CountTrigger.of(2))
      .fold(("", 0)) ({(accumulator, person) => (person.lastName, accumulator._2 + person.age) })
      .map(aggreate => s"Summary age of ${aggreate._1} - ${aggreate._2}\n")

    personReduceStream.writeToSocket(host, new Integer(port), new SimpleStringSchema())

    env.execute("Window aggregations with trigger")

    logger.info("Done")
  }
}
