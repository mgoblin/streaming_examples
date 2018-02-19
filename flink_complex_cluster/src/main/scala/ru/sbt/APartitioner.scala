package ru.sbt

import org.apache.flink.api.common.functions.Partitioner

class APartitioner extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = numPartitions match {
    case x if x > 1 =>
      if (key.toLowerCase.startsWith("a"))
        0
      else
        1
    case _ => 0
  }
}
