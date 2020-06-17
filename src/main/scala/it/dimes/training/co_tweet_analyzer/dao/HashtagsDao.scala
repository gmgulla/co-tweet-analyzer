package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

class HashtagsDao private (_sqlSession: SparkSession, _rootPath: String) extends AbstractDao(_sqlSession, _rootPath) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val HASHTAGS_PATH = "hashtags/Hashtags.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$rootPath/$HASHTAGS_PATH")
    renameColumn(data)
  }

  private def renameColumn(dataToRenameColumn: Dataset[Row]): Dataset[Row] = {
    dataToRenameColumn
      .withColumnRenamed("status_id", "tweet")
      // hashtag - column name not modified
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object HashtagsDao {

  private var singleton: Option[HashtagsDao] = None

  def apply(_sqlSession: SparkSession, _rootPath: String): HashtagsDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsDao(_sqlSession, _rootPath))
    }
    singleton.get
  }

}
