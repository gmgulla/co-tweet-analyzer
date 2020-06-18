package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Defines DAO to read `Tweets` dataset from files conteided by `data/tweets/` directory.
 *
 * @author Gian Marco Gullà
 */
class TweetsDao private (_sqlSession: SparkSession, _rootPath: String) extends AbstractDao(_sqlSession, _rootPath) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /* For convenience, only one file is read during development
   * Leave only 'tweet' directory path for reading (and analyzing) whole tweets dataset
   */
  /**
   * Path for its specific dataset file to read
   */
  private val TWEETS_PATH = "tweets/2020-03-13 Coronavirus Tweets.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * @inheritdoc
   * @return dataset to query
   */
  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$rootPath/$TWEETS_PATH")
    renameColumn(data)
  }

  /**
   * Renames columns to more useful names.
   */
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

/**
 * Factory for singleton [[TweetsDao]]
 */
object TweetsDao {

  private var singleton: Option[TweetsDao] = None

  /**
   * Provides to implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession `SaprkSession` to use
   * @param _rootPath data directory root path
   * @return singleton [[HashtagsDao]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): TweetsDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new TweetsDao(_sqlSession, _rootPath))
    }
    singleton.get
  }
}
