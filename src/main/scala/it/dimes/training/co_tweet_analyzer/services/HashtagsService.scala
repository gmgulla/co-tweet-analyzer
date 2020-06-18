package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.HashtagsDao
import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * It's responsible for quering `Hashtags` dataset.
 *
 * @author Gian Marco Gullà
 */
class HashtagsService private (_dao: HashtagsDao) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * data access object to use to read dataset
   */
  private val dao = _dao

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /*
   * ============================================================
   * QUERY
   * Calculate Hashtags
   * ============================================================
   */

  def calculateHashtags(): Map[String, Long] = {
    val hashtagColumnName = "hashtag"
    val countColumnName = "count"
    val hashtags = dao.readData()
    hashtags.select(hashtagColumnName)
      .groupBy(hashtagColumnName)
      .count().alias(countColumnName)
      .map(row =>
        (row.getAs[String](hashtagColumnName), row.getAs[Long](countColumnName))
      ) (Encoders.tuple[String, Long](Encoders.STRING, Encoders.scalaLong))
      .filter(row => row._1 != null && ! row._1.startsWith("<"))
      .collect()
      .toMap
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

/**
 * FActory for [[HashtagsService]]
 */
object HashtagsService {

  private var singleton: Option[HashtagsService] = None

  /**
   * Implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession `SaprkSession` to use
   * @param _rootPath data directory root path
   * @return singleton [[HashtagsService]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): HashtagsService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsService(HashtagsDao(_sqlSession, _rootPath)))
    }
    singleton.get
  }
}
