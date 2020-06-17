package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

class CountriesDao private (_sqlSession: SparkSession, _rootPath: String) extends AbstractDao(_sqlSession, _rootPath) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val COUNTRIES_PATH = "countries/Countries.pq"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override def readData(): Dataset[Row] = {
    val data = sqlSession.read.parquet(s"$rootPath/$COUNTRIES_PATH")
    renameColumn(data)
  }


  private def renameColumn(dataToRenameColumn: Dataset[Row]): Dataset[Row] = {
    dataToRenameColumn
      .withColumnRenamed("country", "name")
      .withColumnRenamed("country_code", "code")
  }

}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object CountriesDao {

  private var singleton: Option[CountriesDao] = None

  def apply(_sqlSession: SparkSession, _rootPath: String): CountriesDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new CountriesDao(_sqlSession, _rootPath))
    }
    singleton.get
  }

}
