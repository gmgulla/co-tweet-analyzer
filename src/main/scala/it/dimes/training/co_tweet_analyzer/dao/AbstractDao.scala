package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Defines a common base for a DAO ('Data Access Object') class.
 * It provides an abstract methods to read dataset file(s).
 *
 * @param _sqlSession `SaprkSession` to use
 * @param _rootPath data directory root path
 *
 * @author Gian Marco Gullà
 */
abstract class AbstractDao protected(_sqlSession: SparkSession,  _rootPath: String) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * Data directory root path
   */
  protected val rootPath: String = _rootPath


  /**
   * session to be used for reading data and running queries
   */
  protected val sqlSession: SparkSession = _sqlSession

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * Reads dataset from the corresponding ''parquet'' file(s)
   *
   * @return dataset to query
   */
  protected def readData(): Dataset[Row]

}
