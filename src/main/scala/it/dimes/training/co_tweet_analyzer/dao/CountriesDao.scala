package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Defines DAO to read `Countries` dataset from `data/countries/Countries.pq` file
 *
 * @author Gian Marco Gullà
 */
class CountriesDao private (_sqlSession: SparkSession, _rootPath: String) extends AbstractDao(_sqlSession, _rootPath) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * Path for its specific dataset file to read
   */
  private val COUNTRIES_PATH = "countries/Countries.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  /**
   * @inheritdoc
   *
   * @return dataset to query
   */
  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$rootPath$COUNTRIES_PATH")
    renameColumn(data)
  }


  /**
   * Renames columns to more useful names.
   */
  private def renameColumn(dataToRenameColumn: Dataset[Row]): Dataset[Row] = {
    dataToRenameColumn
      .withColumnRenamed("country", "name")
      .withColumnRenamed("country_code", "code")
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

/**
 * Factory for singleton [[CountriesDao]]
 */
object CountriesDao {

  private var singleton: Option[CountriesDao] = None

  /**
   * Provides to implements the ''singleton'' pattern for this class.
   *
   * @param _sqlSession `SaprkSession` to use
   * @param _rootPath data directory root path
   * @return singleton [[CountriesDao]]
   */
  def apply(_sqlSession: SparkSession, _rootPath: String): CountriesDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new CountriesDao(_sqlSession, _rootPath))
    }
    singleton.get
  }

}
