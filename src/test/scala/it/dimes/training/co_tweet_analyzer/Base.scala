package it.dimes.training.co_tweet_analyzer

import it.dimes.training.co_tweet_analyzer.services.{CountriesService, HashtagsService, TweetsService}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Base extends App {

  // Init Spark Context
  val conf = new SparkConf()
  conf.setAppName("CoTweetAnalyzer")
  conf.setMaster("local")
  val spark = new SparkContext(conf)

  // Init SQL Spark Session
  val session = SparkSession.builder().getOrCreate()


  println("================== Countries TEST======================================")
  val countriesService = CountriesService(session)
  countriesService.printSchema()
  countriesService.showAll()
  println("================== Tweets TEST======================================")
  val tweetsService = TweetsService(session)
  tweetsService.printSchema()
  tweetsService.showOriginal()
  tweetsService.showOriginal(150)

  println("================== Hashtag TEST======================================")
  val hashtagService = HashtagsService(session)
  hashtagService.printSchema()
  hashtagService.show(100)
  hashtagService.show()

  println("================== Singleton TEST======================================")
  val otherCS = CountriesService(session)
  println(s"${countriesService.toString}\n${otherCS.toString}")
  val otherTS = TweetsService(session)
  println(s"${tweetsService.toString}\n${otherTS.toString}")
  val otherHS = HashtagsService(session)
  println(s"${hashtagService.toString}\n${otherHS.toString}")





}
