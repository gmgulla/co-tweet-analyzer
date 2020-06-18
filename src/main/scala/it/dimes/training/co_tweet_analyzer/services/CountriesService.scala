package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.CountriesDao
import org.apache.spark.sql.SparkSession

/**
 * It's responsible for quering `Countries` dataset.
 *
 * @note at the moment, this class doesn't implements any methods to execute queries
 *
 * @author Gian Marco Gullà
 */
class CountriesService private (_dao: CountriesDao) {

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

  // TODO: Implement queries

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

/**
 * Factory for [[CountriesService]]
 */
object CountriesService {

  private var singleton: Option[CountriesService] = None

  /**
   * Implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession
   * @return singleton [[TweetsService]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): CountriesService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new CountriesService(CountriesDao(_sqlSession, _rootPath)))
    }
    singleton.get
  }
}
