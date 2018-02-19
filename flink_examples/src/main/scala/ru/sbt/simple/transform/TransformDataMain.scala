package ru.sbt.simple.transform

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.util.Collector

/**
  * Примеры операторов трасформации данных
  *
  *
  * Перед запуском приложения необходимо стартовать сокет сервер
  * при использовании пакета Nmap для windows сокет сервер запускается командой ncat -lkv 127.0.0.1 9999
  * при использовании linux можно использовать команду nc -lkv 127.0.0.1 9999
  */
object TransformDataMain extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketTextStream = env.socketTextStream("127.0.0.1", 9999)

    // Map
    val mappedStream = socketTextStream.map( s => s.reverse )

    mappedStream.addSink(s => logger.info(s"Map: $s"))

    // Filter
    socketTextStream
      .filter(s => s.contains("s"))
      .addSink(s => logger.info(s"Filter: $s"))

    // FlatMap
    socketTextStream
      .flatMap(s => s match {
          case "send" => Array("send")
          case _ =>  s.sliding(1)
        })
      .addSink(s => logger.info(s"FlatMap: $s"))


    // Process
    socketTextStream
      .process(new ProcessFunction[String, String] {
        override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]) = {
          val capitalizedValue = value.capitalize
          out.collect(capitalizedValue)
        }
      })
      .addSink(s => logger.info(s"Process: $s"))

    // Transform
    socketTextStream
      .transform("BraveNewOperator", new AbstractStreamOperator[String] with OneInputStreamOperator[String, String] {
        override def processElement(element: StreamRecord[String]): Unit = {
          val oldValue = element.getValue
          val newValue = oldValue.reverse
          val newElement = element.replace(newValue)

          this.output.collect(newElement)
        }
      })
      .addSink(s => logger.info(s"Transform: $s"))

    env.execute("Simple data transformations")

    logger.info("Done")

  }

}
