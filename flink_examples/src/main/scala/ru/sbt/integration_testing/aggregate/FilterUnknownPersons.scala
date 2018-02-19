package ru.sbt.integration_testing.aggregate

import org.apache.flink.api.common.functions.FilterFunction
import ru.sbt.person.Person

class FilterUnknownPersons extends  FilterFunction[Person] {
  override def filter(p: Person): Boolean = p != Person.unknown
}
