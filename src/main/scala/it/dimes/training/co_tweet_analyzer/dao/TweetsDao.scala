package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}

class TweetsDao private (_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val TWEETS_PATH = "coronavirus-covid19-tweets/2020-03-12 Coronavirus Tweets.CSV"
  override val data: Dataset[Row] = readData()

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override protected def readData(): Dataset[Row] = {
    val schema = buildSchema()
    val dataset = sqlSession.read
      .option("head", "true")
      .schema(schema)
      .csv(s"$RES_PATH$TWEETS_PATH")
    clean(dataset)
  }

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

  override protected def clean(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.filter(dataset("tweetId").isNotNull)
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
