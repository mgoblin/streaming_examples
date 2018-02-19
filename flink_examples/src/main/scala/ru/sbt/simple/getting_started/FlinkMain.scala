package ru.sbt.simple.getting_started

import com.typesafe.scalalogging.LazyLogging

// Для корректной компиляции необходимо импортировать API
import org.apache.flink.api.scala._

/**
  * Первое приложение Apache Flink
  * приложение не является потоковым
  */
object FlinkMain extends LazyLogging {
  /**
    * Приложения является обычной Java программой с методом main
    * @param args аргументы командной строки
    */
  def main(args: Array[String]): Unit = {
    logger.info("Application starting")

    // Получить среду исполнения. В данном примере среда исполнения локальная, следовательно
    // приложение может быть запущено только в среде разработки или как standalone
    // и не будет работать в кластере Apache Flink
    val env = ExecutionEnvironment.createLocalEnvironment(ExecutionEnvironment.getDefaultLocalParallelism)

    // Получить и распечатать в консоль список чисел
    val numbers = env.fromCollection(List(1, 2, 3, 4, 5))
    numbers.print()

    logger.info("Application started")
  }
}
