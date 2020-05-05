package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.dao.HashtagsDao
import org.apache.spark.sql.SparkSession

class HashtagsService private (_dao: HashtagsDao) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val dao = _dao

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  def printSchema(): Unit = {
    dao.data.printSchema()
  }

  def show(): Unit = {
    dao.data.show()
  }

  def show(numRows: Int): Unit = {
    dao.data.show(numRows, false)
  }

  override def toString = s"${super.toString}\n${dao.toString}"

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
