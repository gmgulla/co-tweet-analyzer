package it.dimes.training.co_tweet_analyzer.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

class CountriesDao private (_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

// -----------------------------------------------------------------------||
// FIELDS ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

  private val COUNTRIES_PATH = "other/Countries.CSV"

// -----------------------------------------------------------------------||
// METHODS ---------------------------------------------------------------||
// -----------------------------------------------------------------------||

  override def readData(): Dataset[Row] = {
    val schema = buildSchema()
    val dataset = sqlSession.read
      .option("header", "true")
      .schema(schema)
      .csv(s"$RES_PATH$COUNTRIES_PATH")
    clean(dataset)
  }


  override protected def buildSchema(): StructType = {
    new StructType()
      .add("name", StringType, true)
      .add("code", StringType, true)
  }

  protected def clean(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.filter(dataset("code").isNotNull && dataset("name").isNotNull )
  }


}

// -----------------------------------------------------------------------||
// OBJECT ----------------------------------------------------------------||
// -----------------------------------------------------------------------||

object CountriesDao {

  private var singleton: Option[CountriesDao] = None

  def apply(_sqlSession: SparkSession): CountriesDao = {
    singleton match {
      case Some(_) =>
      case None => singleton = Some(new CountriesDao(_sqlSession))
    }
    singleton.get
  }

}
