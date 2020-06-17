package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

abstract class AbstractDao protected(_sqlSession: SparkSession,  _rootPath: String) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  protected val rootPath = _rootPath
  protected val sqlSession = _sqlSession

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  protected def readData(): Dataset[Row]

}
