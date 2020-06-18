package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Defines DAO to read `Hashtags` dataset from `data/hashtags/Hashtags.pq` file.
 *
 * @author Gian Marco Gullà
 */
class HashtagsDao private (_sqlSession: SparkSession, _rootPath: String) extends AbstractDao(_sqlSession, _rootPath) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * Path for its specific dataset file to read
   */
  private val HASHTAGS_PATH = "hashtags/Hashtags.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * @inheritdoc
   * @return dataset to query
   */
  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$rootPath/$HASHTAGS_PATH")
    renameColumn(data)
  }

  /**
   * Renames columns to more useful names.
   */
  private def renameColumn(dataToRenameColumn: Dataset[Row]): Dataset[Row] = {
    dataToRenameColumn
      .withColumnRenamed("status_id", "tweet")
      // hashtag - column name not modified
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

/**
 * Factory for singleton [[HashtagsDao]]
 */
object HashtagsDao {

  private var singleton: Option[HashtagsDao] = None

  /**
   * Provides to implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession `SaprkSession` to use
   * @param _rootPath data directory root path
   * @return singleton [[HashtagsDao]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): HashtagsDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsDao(_sqlSession, _rootPath))
    }
    singleton.get
  }

}
