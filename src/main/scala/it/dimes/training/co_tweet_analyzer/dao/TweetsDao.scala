package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}

class TweetsDao private (_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  // For convenience, only one file is read during development
  private val TWEETS_PATH = "/tweets/2020-03-13 Coronavirus Tweets.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$TWEETS_PATH")
    renameColumn(data)
  }

  private def renameColumn(dataToRenameColumn: Dataset[Row]): Dataset[Row] = {
    dataToRenameColumn
      .withColumnRenamed("status_id", "id")
      .withColumnRenamed("user_id", "userId")
      .withColumnRenamed("created_at", "date")
      .withColumnRenamed("screen_name", "userName")
      // text - column name not modified
      // source - column name not modified
      .withColumnRenamed("reply_to_status_id", "replyToId")
      .withColumnRenamed("reply_to_user_id", "replyToUserId")
      .withColumnRenamed("reply_to_screen_name", "replyToUserName")
      .withColumnRenamed("is_quote", "isQuote")
      .withColumnRenamed("is_retweet", "isRetweet")
      .withColumnRenamed("favourites_count", "likesCount")
      .withColumnRenamed("retweet_count", "retweetCount")
      .withColumnRenamed("country_code", "countryCode")
      .withColumnRenamed("place_full_name", "placeFullName")
      .withColumnRenamed("place_type", "placeType")
      .withColumnRenamed("followers_count","followerCount")
      .withColumnRenamed("friends_count", "friendsCount")
      .withColumnRenamed("account_lang", "accountLang")
      .withColumnRenamed("account_created_at", "accountCreationDate")
      .withColumnRenamed("verified", "isVerified")
      .withColumnRenamed("lang", "tweetLang")

  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object TweetsDao {

  private var singleton: Option[TweetsDao] = None

  def apply(_sqlSession: SparkSession): TweetsDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new TweetsDao(_sqlSession))
    }
    singleton.get
  }
}
