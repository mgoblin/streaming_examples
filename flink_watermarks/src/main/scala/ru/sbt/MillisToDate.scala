package ru.sbt

import java.time.{Instant, LocalDateTime, ZoneId}

import com.typesafe.scalalogging.LazyLogging

object MillisToDate extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val millis = 1517216925230L

    val date = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(millis),
      ZoneId.systemDefault()
    )

    println(date)
  }
}
