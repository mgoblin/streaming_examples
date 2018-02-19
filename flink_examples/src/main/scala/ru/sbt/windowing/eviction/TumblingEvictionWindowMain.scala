package ru.sbt.windowing.eviction

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.util.serialization.SerializationSchema
import ru.sbt.person.Person



object TumblingEvictionWindowMain extends LazyLogging {

  type FamilyAgeSum = (String, Int)
  val emptyFamilyAgeSum: FamilyAgeSum = ("", 0)

  val host = "localhost"
  val inPort = 9999
  val outPort = 9998
  val WindowSizeSeconds = 5

  val unknownPersonsEvictor =
    new Evictor[Person, TimeWindow] {
      override def evictBefore(
        elements: lang.Iterable[TimestampedValue[Person]],
        size: Int,
        window: TimeWindow,
        evictorContext: Evictor.EvictorContext): Unit = {

        logger.info("Call evictBefore")

        val elementsIterator = elements.iterator
        while(elementsIterator.hasNext) {
          val element = elementsIterator.next()
          element.getValue match {
            case (Person.unknown | Person("Duncan", "Macleod", _)) =>
              elementsIterator.remove()
            case _ =>
          }
        }
      }

      override def evictAfter(
        elements: lang.Iterable[TimestampedValue[Person]],
        size: Int,
        window: TimeWindow,
        evictorContext: Evictor.EvictorContext): Unit = logger.info("Call evictAfter")
    }

  def main(args: Array[String]): Unit = {
    logger.info("Starting windowing triggered aggregation application")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 2)
    val socketTextStream = env.socketTextStream(host, inPort)


    val personStream = socketTextStream
      // String => Person
      .map(s => Person(s).recover{
      // В случае исключительной ситуации залогировать ошибку и вернуть неизвестную персону
      case ex: Exception =>
        logger.error(s"Error: ${ex.getMessage}")
        Person.unknown
    }.get)

    personStream.addSink(person => logger.info(s"$person arrived"))

    val familyPersonStream: DataStream[FamilyAgeSum] = personStream
      .keyBy("lastName")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(WindowSizeSeconds)))
      .evictor(unknownPersonsEvictor)
      .fold[FamilyAgeSum](emptyFamilyAgeSum) { (acc: FamilyAgeSum, person: Person) => (person.lastName, acc._2 + person.age)}
      .filter(fsm => fsm != emptyFamilyAgeSum)


    familyPersonStream
      .writeToSocket(host, outPort, new SerializationSchema[FamilyAgeSum] {
        def serialize(element: FamilyAgeSum) = {
          val stringRepr = s"Persons with last name '${element._1}' has ${element._2} sum of ages\n"
          stringRepr.getBytes
        }
      })

    env.execute("Window aggregations with trigger")

    logger.info("Done")
  }
}
