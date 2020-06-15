package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.TweetsDao
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}


class TweetsService private (_dao: TweetsDao) {

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
   * Calculate languages
   * ============================================================
   */

  def calculateLangueges(): Map[String, Long] = {
    val langColumnName =  "lang" //"tweetLang"
    val countColumnName = "count"
    val tweets = dao.readData()
    tweets.select(langColumnName)
      .groupBy(langColumnName)
      .count().alias(countColumnName)
      .map(row =>
        (row.getAs[String](langColumnName), row.getAs[Long](countColumnName))
      ) (Encoders.tuple[String, Long](Encoders.STRING, Encoders.scalaLong))
      .collect()
      .toMap
  }

  /*
   * ============================================================
   * QUERY
   * Calculate sources (devices)
   * ============================================================
   */


  def calculateSources(): Map[String, Long] = {
    val sourceColunmName = "source"
    val countColumnName = "count"
    val tweets = dao.readData()
    tweets.select(sourceColunmName)
      .groupBy(sourceColunmName)
      .count().alias(countColumnName)
      .map(row =>
        (row.getAs[String](sourceColunmName), row.getAs[Long](countColumnName))
      ) (Encoders.tuple[String, Long](Encoders.STRING, Encoders.scalaLong))
      .collect()
      .toMap
  }

  /*
   * ============================================================
   * QUERY
   * Calculate user names
   * ============================================================
   */

  def calculateUserNames(): Map[String, Long] = {
    val userColumnName = "screen_name"  //"userName"
    val countColumnName = "count"
    val tweets = dao.readData()
    tweets.select(userColumnName)
      .groupBy(userColumnName)
      .count().alias(countColumnName)
      .map(row =>
        (row.getAs[String](userColumnName), row.getAs[Long](countColumnName))
      ) (Encoders.tuple[String, Long](Encoders.STRING, Encoders.scalaLong))
      .collect()
      .toMap
  }

  /*
  def calculateUserNamesRDD(): scala.collection.Map[String, Int] = {
    val userColumnName = "screen_name"  //"userName"
    val countColumnName = "count"
    val tweets = dao.readData()
    tweets.select(userColumnName)
      .rdd
      .map(row => (row.toString(), 1))
      .reduceByKey(_+_)
      .collectAsMap()
  }
   */

  /*
  // Implemented only for testing
  def getLanguages(): DataFrame = {
    val sourceColumName = "tweetLang"
    val tweets = dao.readData()
    tweets.select(sourceColumName)
  }
   */

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object TweetsService {

  private var singleton: Option[TweetsService] = None

  def apply(_sqlSession: SparkSession): TweetsService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new TweetsService(TweetsDao(_sqlSession)))
    }
    singleton.get
  }

}
