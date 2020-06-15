package it.dimes.training.co_tweet_analyzer.dao

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class HashtagsDaoTest {

  var sqlSession: SparkSession = _

  @Before
  def setUp(): Unit = {
      sqlSession = Launcher.init("Hashtags Dao Test", "local[*]")
  }

  @Test
  def isHashtagsDatasetRead(): Unit = {
    val data = HashtagsDao(sqlSession).readData()
    data.printSchema()
    data.show(200)
    Assert.assertNotNull(data)
  }
}
