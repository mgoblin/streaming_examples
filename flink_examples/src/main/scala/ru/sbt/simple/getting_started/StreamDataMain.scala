package ru.sbt.simple.getting_started

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._

/**
  * Простейшее приложение потоковой обработки
  *
  * Перед запуском приложения необходимо стартовать сокет сервер
  * при использовании пакета Nmap для windows сокет сервер запускается командой ncat -lkv 127.0.0.1 9999
  * при использовании linux можно использовать команду nc -lkv 127.0.0.1 9999
  */
object StreamDataMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting data transformation application")

    // получить локальную среду исполнения.
    // запуск возможен только в среде разработки или как standalone приложение
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)

    // Читать пользовательский ввод из сокета и печатать в консоль
    val socketTextStream = env.socketTextStream("localhost", 9999)
      .print()

    // Старт выполнения приложения
    env.execute("Listening")

    logger.info("Done")

  }
}
