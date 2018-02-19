package ru.sbt.integration_testing.aggregate

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test
import ru.sbt.integration_testing.aggregate.MaxByMain.personsMaxByAgeFlow
import ru.sbt.person.Person

class MaxByTests extends StreamingMultipleProgramsTestBase {

  @Test def testMaxByAgeEmpty(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    CollectSink.values.clear()

    personsMaxByAgeFlow(env.fromElements())
      .addSink(new CollectSink)

    env.execute()

    val expected = new CopyOnWriteArrayList[Person]

    assertEquals(expected, CollectSink.values)
  }

  @Test def testMaxByAgeOneElement(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    CollectSink.values.clear()

    personsMaxByAgeFlow(env.fromElements("mike, g, 15"))
      .addSink(new CollectSink)

    env.execute()

    val expected = new CopyOnWriteArrayList[Person]
    expected.add(Person("mike", "g", 15))

    assertEquals(expected, CollectSink.values)
  }

  @Test def testMaxByAgeManyLastNameElements(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    CollectSink.values.clear()

    personsMaxByAgeFlow(env.fromElements(
      "mike,g, 15",
      "alex,g, 25",
      "helen, x, 22",
      "peter,g, 15",
      "vlad, x, 13"
    )).addSink(new CollectSink)

    env.execute()

    val expected = new CopyOnWriteArrayList[Person]
    expected.add(Person("mike", "g", 15))
    expected.add(Person("alex", "g", 25))
    expected.add(Person("helen", "x", 22))
    expected.add(Person("alex", "g", 25))
    expected.add(Person("helen", "x", 22))

    assertEquals(expected, CollectSink.values)
  }
}