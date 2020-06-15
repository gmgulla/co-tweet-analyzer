package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class CountriesDaoTest {

  var sqlSession: SparkSession = _

  @Before
  def setUp(): Unit = {
    sqlSession = Launcher.init("Countries Dao Test", "local[*]")
  }

  @Test
  def isCountriesDatasetRead(): Unit = {
    val data = CountriesDao(sqlSession).readData()
    data.printSchema()
    data.show(200, true)
    Assert.assertNotNull(data)
  }

}
