package ru.sbt.windowing.nonkeyed

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.person.PersonStream

object SlidingWindowMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting windowing aggregation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)
    val socketTextStream = env.socketTextStream("localhost", 9999)

    val personReduceStream = PersonStream(socketTextStream)
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(15)))
      .reduce((p1, p2) =>
        p1.copy(firstName = p1.firstName + "-->" + p2.firstName)
          .copy(lastName = p1.lastName + "-->" + p2.lastName)
          .copy(age = p1.age  + p2.age)
      )

    personReduceStream.addSink(s => logger.info(s"Sliding windowAll reduce: $s"))

    env.execute("Window aggregations")

    logger.info("Done")
  }
}
