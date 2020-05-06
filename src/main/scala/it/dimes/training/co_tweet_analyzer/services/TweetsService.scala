package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.TweetsDao
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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
   * Get most used languages
   * ============================================================
   */

  def getMostUsedLangueges(): DataFrame = {
    val tweets = dao.data
    val dirtyMostUsedLanguages = tweets.filter(tweets("tweetLang").isNotNull)
      .groupBy("tweetLang")
      .agg(functions.count("tweetLang").alias("count"))
    dirtyMostUsedLanguages.where(
      dirtyMostUsedLanguages("tweetLang").rlike("[a-z]{2}")
    )
  }

  /*
   * ============================================================
   * Print methods
   * ============================================================
   */

  def showOriginal(): Unit = {
    val numRows = 20
    showOriginal(numRows)
  }

  def showOriginal(numRows: Int): Unit = {
    show(dao.data, numRows)
  }

  def showAll(dataFrame: DataFrame): Unit = {
    val numRows = dataFrame.count().toInt
    show(dataFrame, numRows)
  }

  def show(dataFrame: DataFrame, numRows: Int): Unit = {
    val truncate = false
    dataFrame.show(numRows, truncate)
  }

  def printSchema(): Unit = {
    dao.data.printSchema()
  }


  override def toString = s"${super.toString}\n${dao.toString}"
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
