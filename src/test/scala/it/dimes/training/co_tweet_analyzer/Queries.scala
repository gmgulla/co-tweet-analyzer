package it.dimes.training.co_tweet_analyzer

import it.dimes.training.co_tweet_analyzer.services.TweetsService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Queries extends App {

  // Init Spark Context
  val conf = new SparkConf()
  conf.setAppName("CoTweetAnalyzer")
  conf.setMaster("local")
  val spark = new SparkContext(conf)

  // Init SQL Spark Session and Services
  val session = SparkSession.builder().getOrCreate()
  val tweetsService = TweetsService(session)

  // Most Used Languages
  println("== QUERY: Most Used Languages ==================================")
  val mostUsedLangueges = tweetsService.getMostUsedLangueges()
  mostUsedLangueges.show(mostUsedLangueges.count().toInt, false)

}
