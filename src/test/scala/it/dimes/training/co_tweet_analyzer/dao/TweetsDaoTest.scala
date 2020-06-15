package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class TweetsDaoTest {

  var sqlSession: SparkSession = _

  @Before
  def setUp: Unit = {
    sqlSession = Launcher.init("Tweets Dao Test", "local[*]")
  }

  @Test
  def isTweetsDatasetRead(): Unit = {
    val data = TweetsDao(sqlSession).readData()
    data.printSchema()
    //df.collect().take(200).foreach(println)
    data.show(200, true)
    Assert.assertNotNull(data)
  }

}
