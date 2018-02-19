package ru.sbt.simple.aggregate.max

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import ru.sbt.person.Person


object MaxByMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)

    val socketPersonsStream = env.socketTextStream("localhost", 9999)

    socketPersonsStream
      .map(s => Person(s).recover {
        case ex: Exception => {
          logger.error(s"Error: ${ex.getMessage}")
          Person.unknown
        }
      }.get)
      .filter(p => p != Person.unknown)
      .keyBy("lastName")
      .maxBy("age")
      .addSink(s => logger.info(s"Самый старшний ${s.lastName} - это ${s.firstName}. Его(ее) возраст ${s.age}"))

    env.execute("MaxBy")

    logger.info("Done")
  }
}
