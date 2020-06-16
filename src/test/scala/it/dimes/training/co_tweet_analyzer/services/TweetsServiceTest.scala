package it.dimes.training.co_tweet_analyzer.services

import it.dimes.training.co_tweet_analyzer.Launcher
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.junit.Assert._

class TweetsServiceTest {

  var sqlSession: SparkSession = _

  @Before
  def setUp: Unit = {
    sqlSession = Launcher.init("Tweets Service Test", "local")
  }

  @Test
  def isCalculateSourceReturnedValueCorrect(): Unit = {
    val tweetsService = TweetsService(sqlSession)
    val data = tweetsService.calculateSources()
    assertNotNull(data)
    data.foreach(println)
  }

  @Test
  def isCalculateLanguagesReturnedValueCorrect(): Unit = {
    val tweetsService = TweetsService(sqlSession)
    val data = tweetsService.calculateLangueges()
    assertNotNull(data)
    data.foreach(println)
  }

  @Test
  def isCalculateUserNamesReturnedValueCorrect(): Unit = {
    val tweetsService = TweetsService(sqlSession)
    val data = tweetsService.calculateUserNames()
    assertNotNull(data)
    data.foreach(println)
  }

}
