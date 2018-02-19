package ru.sbt.windowing.keyed

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.person.PersonStream

object TumblingWindowMain2 extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting windowing aggregation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 2)
    val socketTextStream = env.socketTextStream("localhost", 9999)

    val personReduceStream = PersonStream(socketTextStream)
      .keyBy("lastName")
      // партиционирование потока по фамилии персоны.
      // Все, вводимые в 30 секундные интервалы персоны с одной фамилией попадут в один экземпляр окна и
      // один узел обработки окна
      .timeWindow(Time.seconds(30))
      .reduce((p1, p2) =>
        p1
          .copy(firstName = p1.firstName + "-->" + p2.firstName)
          .copy(lastName = p1.lastName + "-->" + p2.lastName)
          .copy(age = p1.age + p2.age)
      )

    personReduceStream.addSink(s => logger.info(s"KeyBy window reduce: $s"))

    env.execute("Window aggregations")

    logger.info("Done")
  }
}
