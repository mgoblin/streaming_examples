package ru.sbt

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._

object CheckpointsMain extends LazyLogging {

  val keyString: String => String = _.substring(0, 2)
  val lengthField: String = "_3"

  val checkpointFilePath = "file:///C:\\TEMP\\flink\\FsBackend"

  def main(args: Array[String]): Unit = {
    logger.info("Checkpointing example starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    //env.setStateBackend(new MemoryStateBackend(10 * 1024, true))
    //env.setStateBackend(new FsStateBackend(checkpointFilePath))
    env.setStateBackend(new RocksDBStateBackend(checkpointFilePath))

    val stringStream = env.socketTextStream("127.0.0.1", 9999)
      .name("Socket reader")
      .map(s => (s, keyString(s), s.length))
      .keyBy(s => s._2)
      .maxBy(lengthField)

    stringStream
      .addSink(s => logger.info(s"Max length string starting by '${s._2}' is ${s._3} and this is '${s._1}'"))
      .name("Sink to logger")

    env.execute("Checkpointing example")

    logger.info("Done")
  }
}
