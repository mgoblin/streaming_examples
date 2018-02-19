package ru.sbt.person

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._

/**
  * Объект потока персон
  */
object PersonStream extends LazyLogging {
  /**
    * Метод трансформации потока строк в поток персон.
    *
    * Строковый ввод, который не может быть распознан как персона игнорируется.
    *
    * @param dataStream поток строк
    * @return поток персон
    */
  def apply(dataStream: DataStream[String]): DataStream[Person] =
    dataStream
      // String => Person
      .map(s => Person(s).recover{
        // В случае исключительной ситуации залогировать ошибку и вернуть неизвестную персону
        case ex: Exception =>
          logger.error(s"Error: ${ex.getMessage}")
          Person.unknown
      }.get)
      // Отфильтровать неизвестных персон
      .filter(person => person != Person.unknown)
}
