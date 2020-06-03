package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.HashtagsDao
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
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

  def calculateHashtags(): DataFrame = {
    val hashtags = dao.readData()
    val hashtagColumnName = "hashtag"
    hashtags.filter(hashtags(hashtagColumnName).isNotNull)
      .groupBy(hashtagColumnName)
      .agg(functions.count(hashtagColumnName))
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object HashtagsService {

  private var singleton: Option[HashtagsService] = None

  def apply(_sqlSession: SparkSession): HashtagsService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new HashtagsService(HashtagsDao(_sqlSession)))
    }
    singleton.get
  }
}
