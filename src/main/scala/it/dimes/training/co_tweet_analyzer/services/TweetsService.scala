package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.TweetsDao
import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * It's responsible for quering `Tweets` dataset.
 *
 * @author Gian Marco Gullà
 */
class TweetsService private (_dao: TweetsDao) {

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
   * Calculate languages
   * ============================================================
   */

  /**
   * Executes query to calculate, for each language, how many tweets have been published.
   *
   * @return a map whose key is a language and value is how many tweets
   *         have beenn published in that language
   */
  def calculateLangueges(): Map[String, Long] = {
    val langColumnName =  "tweetLang"
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

  /**
   * Executes query to calculate, for each source, how many tweets have been published.
   * A ''source'' is the platform which through tweets was posted (like Twitter Web App,
   * Twitter iOS app, or any other third-party app or service).
   *
   * @return a map whose key is a source and value is how many tweets
   *         have beenn published through that source
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

  /**
   * Executes query to calculate how many tweets every user, present in the dataset,
   * has published. A ''source'' is the platform which through tweets was posted (like Twitter Web App,
   * Twitter iOS app, or any other third-party app or service).
   *
   * @return a map whose key is a source and value is how many tweets
   *         have beenn published through that source
   */
  def calculateUserNames(): Map[String, Long] = {
    val userColumnName = "userName"  //"userName"
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

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

/**
 * Factory for [[TweetsService]]
 */
object TweetsService {

  private var singleton: Option[TweetsService] = None

  /**
   * Implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession `SaprkSession` to use
   * @param _rootPath data directory root path
   * @return singleton [[TweetsService]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): TweetsService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new TweetsService(TweetsDao(_sqlSession, _rootPath)))
    }
    singleton.get
  }

}
