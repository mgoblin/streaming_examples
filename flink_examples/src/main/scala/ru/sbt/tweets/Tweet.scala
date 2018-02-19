package ru.sbt.tweets

import java.util.Date

case class Tweet
(
  userId: String,
  tweetDateTime: Date,
  tags: Seq[String]
) extends Serializable
