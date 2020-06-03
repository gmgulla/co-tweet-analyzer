package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

class HashtagsDao private (_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val HASHTAGS_PATH = "other/Hashtags.CSV"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override protected def buildSchema(): StructType = {
    new StructType()
      .add("tweetId", StringType, true)
      .add("hashtag", StringType, true)
  }

  override def readData(): Dataset[Row] = {
    val schema = buildSchema()
    sqlSession.read
      .option("header", "true")
      .schema(schema)
      .csv(s"$RES_PATH$HASHTAGS_PATH")

  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object HashtagsDao {

  private var singleton: Option[HashtagsDao] = None

  def apply(_sqlSession: SparkSession): HashtagsDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsDao(_sqlSession))
    }
    singleton.get
  }

}
