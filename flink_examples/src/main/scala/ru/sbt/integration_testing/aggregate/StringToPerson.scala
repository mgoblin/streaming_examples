package ru.sbt.integration_testing.aggregate

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.MapFunction
import ru.sbt.person.Person

class StringToPerson extends MapFunction[String, Person] with LazyLogging {
  override def map(value: String) = Person(value).recover {
    case ex: Exception => {
      logger.error(s"Error: ${ex.getMessage}")
      Person.unknown
    }
  }.get
}
