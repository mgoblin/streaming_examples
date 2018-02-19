package ru.sbt.integration_testing.aggregate

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import ru.sbt.person.Person


object MaxByMain extends LazyLogging {

  val stringToPerson: StringToPerson = new StringToPerson
  val filterUnknownPersons = new FilterUnknownPersons

  val personsMaxByAgeFlow = { stream: DataStream[String] =>
    stream
      .map(stringToPerson)
      .filter(filterUnknownPersons)
      .keyBy("lastName")
      .maxBy("age")
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 4)

    val socketPersonsStream = env.socketTextStream("localhost", 9999)

    personsMaxByAgeFlow(socketPersonsStream)
      .addSink((person: Person) => logger.info(s"Самый старший ${person.lastName} - это ${person.firstName}. Его(ее) возраст ${person.age}"))

    env.execute("MaxBy")

    logger.info("Done")
  }
}
