package ru.sbt.person

import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

/**
  * Класс персоны (человека)
  *
  * @param firstName имя
  * @param lastName фамилия
  * @param age возраст. полных лет.
  */
case class Person(firstName: String, lastName:String, age: Int) {
  require(firstName.trim.nonEmpty, "First name should not be empty")
  require(lastName.trim.nonEmpty, "Last name should not be empty")
  require(age >= 0, "Age should not be negative")
  require(age < 200, "Really? Its possible live more than 200 years?")
}

/**
  * Компаньон класса персоны
  */
object Person extends LazyLogging {
  /**
    * Метод создания экземпляра персоны из строки ввода, разделенной запятой
    * @param s строка ввода
    * @return экземпляр персоны или IllegalArgumentException при ошибках парсинга строки
    */
  def apply(s: String) = Try {
    s.split(',') match {
      case Array(firstName, lastName, age) => new Person(firstName.trim, lastName.trim, age.trim.toInt)
      case _ =>
        val errorMessage = s"$s does not correspond <First name>, <Last name>, <age>"
        logger.error(errorMessage)
        throw new IllegalArgumentException(errorMessage)
    }
  }

  /**
    * Неизвестная персона.
    */
  val unknown = new Person("Unknown", "Unknown", 0)
}
