package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.HashtagsDao
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

class HashtagsService private (_dao: HashtagsDao) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

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
      .collect()
      .toMap
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object HashtagsService {

  private var singleton: Option[HashtagsService] = None

  def apply(_sqlSession: SparkSession, _rootPath: String): HashtagsService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsService(HashtagsDao(_sqlSession, _rootPath)))
    }
    singleton.get
  }
}
