package it.dimes.training.co_tweet_analyzer

import it.dimes.training.co_tweet_analyzer.gui.PieChartGUI
import it.dimes.training.co_tweet_analyzer.services.TweetsService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  println("Launching GUI...")
  /*val arrayMostUsedLangueges = ArrayBuffer[Tuple2[String, Long]]()
  mostUsedLangueges.collect().foreach(row =>
   arrayMostUsedLangueges += (row.getAs("tweetLang"), row.getAs("count"))
  )*/

  val mapMostUsedLangueges = new mutable.HashMap[String, Long]()
  for (row <- mostUsedLangueges.collect()) {
    val name: String = row.getAs("tweetLang")
    val value: Long = row.getAs("count")
    mapMostUsedLangueges.+=((name, value))
  }



  (new PieChartGUI("Most Used Languages", mapMostUsedLangueges)).showChart
}
