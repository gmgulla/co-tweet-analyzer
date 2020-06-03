package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}

class TweetsDao private (_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  // For convenience, only one file is read during development
  private val TWEETS_PATH = "coronavirus-covid19-tweets/2020-03-13 Coronavirus Tweets.CSV"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override def readData(): Dataset[Row] = {
    val schema = buildSchema()
    sqlSession.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .schema(schema)
      .load(s"$RES_PATH$TWEETS_PATH")
  }

    /*override protected def buildSchema(): StructType = {
      new StructType()
        .add("status_id", LongType, true)
        .add("user_id", LongType, true)
        .add("created_at", DateType, true)
        .add("screen_name", StringType, true)
        .add("text", StringType, true)
        .add("source", StringType, true)
        .add("reply_to_status_id", LongType, true)
        .add("reply_to_user_id", LongType, true)
        .add("reply_to_screen_name", StringType, true)
        .add("is_quote", BooleanType, true)
        .add("is_retweet", BooleanType, true)
        .add("favourites_count", IntegerType, true)
        .add("retweet_count", IntegerType, true)
        .add("country_code", StringType, true)
        .add("place_full_name", StringType, true)
        .add("place_type", StringType, true)
        .add("followers_count", IntegerType, true)
        .add("friends_count", IntegerType, true)
        .add("account_lang", StringType, true)
        .add("account_created_at", DateType, true)
        .add("verified", BooleanType, true)
        .add("lang", StringType, true)
    }*/

  override protected def buildSchema(): StructType = {
    new StructType()
      .add("tweetId", LongType, true)
      .add("userId", LongType, true)
      .add("tweetDate", DateType, true)
      .add("userName", StringType, true)
      .add("text", StringType, true)
      .add("source", StringType, true)
      .add("replyToTweetId", LongType, true)
      .add("replyToUserId", LongType, true)
      .add("replyToUserName", StringType, true)
      .add("isQuote", BooleanType, true)
      .add("isRetweet", BooleanType, true)
      .add("likeCount", IntegerType, true)
      .add("retweetCount", IntegerType, true)
      .add("countryCode", StringType, true)
      .add("placeFullName", StringType, true)
      .add("placeType", StringType, true)
      .add("followerCount", IntegerType, true)
      .add("friendsCount", IntegerType, true)
      .add("accountLang", StringType, true)
      .add("accountCreationDate", DateType, true)
      .add("isVerified", BooleanType, true)
      .add("tweetLang", StringType, true)
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
