package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.CountriesDao
import org.apache.spark.sql.SparkSession

class CountriesService private (_dao: CountriesDao) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val dao = _dao

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  // TODO: Implement queries

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object CountriesService {

  private var singleton: Option[CountriesService] = None

  def apply(_sqlSession: SparkSession): CountriesService = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new CountriesService(CountriesDao(_sqlSession)))
    }
    singleton.get
  }
}
