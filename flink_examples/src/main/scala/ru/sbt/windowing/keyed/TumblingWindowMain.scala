package ru.sbt.windowing.keyed

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import ru.sbt.person.PersonStream

object TumblingWindowMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting windowing aggregation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 2)
    val socketTextStream = env.socketTextStream("localhost", 9999)

    val personReduceStream = PersonStream(socketTextStream)
      .keyBy("age") // партиционирование потока по возрасту персоны.
                    // Все, вводимые в 30 секундные интервалы персоны с одним возрастом попадут в один экземпляр окна и
                    // один узел обработки окна
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      .reduce((p1, p2) =>
        p1
          .copy(firstName = p1.firstName + "-->" + p2.firstName)
          .copy(lastName = p1.lastName + "-->" + p2.lastName)
      )

    personReduceStream.addSink(s => logger.info(s"KeyBy window reduce: $s"))

    env.execute("Window aggregations")

    logger.info("Done")
  }
}
