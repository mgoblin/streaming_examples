package ru.sbt.integration_testing.aggregate

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import ru.sbt.person.Person

class CollectSink extends SinkFunction[Person] {
  import CollectSink.values
  override def invoke(value: Person) = values.add(value)
}

object CollectSink {
  val values = new CopyOnWriteArrayList[Person]()
}

