package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class CountriesDaoTest {

  var sqlSession: SparkSession = _
  var rootPath: String = _

  @Before
  def setUp(): Unit = {
    sqlSession = Launcher.init("Countries Dao Test", "local[*]")
    rootPath = "/Users/gmg/Documents/data"
  }

  @Test
  def isCountriesDatasetRead(): Unit = {
    val data = CountriesDao(sqlSession, rootPath).readData()
    data.printSchema()
    data.show(200, true)
    Assert.assertNotNull(data)
  }

}
