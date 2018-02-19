package ru.sbt.simple.aggregate.max

import com.typesafe.scalalogging.LazyLogging
import ru.sbt.person.Person

import org.apache.flink.streaming.api.scala._

/**
  * Приложение вычисляет и выводит в лог максимальный возраст персон с совпадающей фамилией и сумму возрастов
  *
  * <p>
  * Перед запуском приложения необходимо стартовать сокет сервер
  * при использовании пакета Nmap для windows сокет сервер запускается командой ncat -lkv 127.0.0.1 9999
  * при использовании linux можно использовать команду nc -lkv 127.0.0.1 9999
  * <p>
  * Ввод данных: &lt;имя&gt;, &lt;фамилия&gt;, &lt;возраст от 0 до 200&gt;
  */
object MaxMain extends LazyLogging {
  def main(args: Array[String]): Unit = {

    logger.info("Starting data transformation application")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketPersonsStream = env.socketTextStream("localhost", 9999)

    // Поток пофамильного вычисления старшей персоны
    socketPersonsStream
      .map(s => Person(s).getOrElse(Person.unknown))
      .filter( _ != Person.unknown )
      .keyBy(_.lastName)
      .max("age")
      // Оператор max не сохраняет элемент, поэтому можно вывести только фамиилю самой старшей персоны
      // Чтобы получить ФИО старшей персоны необходимо использовать оператор maxBy
      .addSink(person => logger.info(s"Возарст самого(ой) старшего ${person.lastName} равен ${person.age}"))

    // Поток пофамильного суммирования возрастов персон
    socketPersonsStream
      .map(s => Person(s).getOrElse(Person.unknown))
      .filter(_ != Person.unknown)
      .keyBy(person => person.lastName)
      .sum("age")
      .addSink(person => logger.info(s"Сумма возрастов персон с фамилией ${person.lastName} равна ${person.age}"))

    env.execute("Max")

    logger.info("Done")
  }
}
