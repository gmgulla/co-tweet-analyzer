package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

abstract class AbstractDao protected(_sqlSession: SparkSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  protected val sqlSession = _sqlSession
  protected val RES_PATH = "/Users/gmg/Documents/data/"


// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  protected def readData(): Dataset[Row]

}
