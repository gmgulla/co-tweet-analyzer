package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class TweetsDaoTest {

  var sqlSession: SparkSession = _
  var rootPath: String = _

  @Before
  def setUp: Unit = {
    sqlSession = Launcher.init("Tweets Dao Test", "local[*]")
    rootPath = "/Users/gmg/Documents/data"
  }

  @Test
  def isTweetsDatasetRead(): Unit = {
    val data = TweetsDao(sqlSession, rootPath).readData()
    data.printSchema()
    //df.collect().take(200).foreach(println)
    data.show(200, true)
    Assert.assertNotNull(data)
  }

}
