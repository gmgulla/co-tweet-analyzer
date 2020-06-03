package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class TweetsDaoTest {

  var sqlSession: SparkSession = _

  @Before
  def setUp: Unit = {
    sqlSession = Launcher.init("Tweets Service Test", "local")
  }

  @Test
  def isCsvFileReadingCorrect(): Unit = {
    val df = TweetsDao(sqlSession).readData()
    df.printSchema()
    df.show(200, false)
    Assert.assertNotNull(df)
  }

}
