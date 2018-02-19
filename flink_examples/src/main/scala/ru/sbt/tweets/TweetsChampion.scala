package ru.sbt.tweets

case class TweetsChampion
(
  userId: String,
  tweetCount: Int = 1
) {
  override def toString = s"Tweet champion is $userId with $tweetCount tweets"
}
