package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

import scala.collection.mutable

class HashtagsServiceTest {

  private var sqlSession: SparkSession = _

  @Before
   def setUp(): Unit = {
    sqlSession = Launcher.init("Hashtags Service Test", "local[*]")
  }

  @Test
  def isCalculateHashtagsReturnedValueCorrect(): Unit = {
    val hashtagsService = HashtagsService(sqlSession)
    val data = hashtagsService.calculateHashtags()
    Assert.assertNotNull(data)
    data.foreach(println)
  }

}
