package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.TweetsDao
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}
import scala.collection.Map

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

  def calculateLangueges(): DataFrame = {
    val tweets = dao.readData()
    val dirtyMostUsedLanguages = tweets.filter(tweets("tweetLang").isNotNull)
      .groupBy("tweetLang")
      .agg(functions.count("tweetLang").alias("count"))
    dirtyMostUsedLanguages.where(
      dirtyMostUsedLanguages("tweetLang").rlike("[a-z][a-z]")
    )
  }

  /*
   * ============================================================
   * QUERY
   * Calculate sources (devices)
   * ============================================================
   */


  def calculateSources(): Map[String, Int] = {
    val sourceColumName = "source"
    val tweets = dao.readData()
    tweets.select(sourceColumName)
      .map(row => row.toString())(Encoders.STRING)
      .rdd
      .map(source => (source, 1))
      .reduceByKey(_ + _)
      .collectAsMap()
  }

  // Implemented only for testing
  def getSources(): DataFrame = {
    val sourceColumName = "source"
    val tweets = dao.readData()
    tweets.select(sourceColumName)
  }

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
