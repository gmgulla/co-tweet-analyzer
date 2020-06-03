package it.dimes.training.co_tweet_analyzer

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Launcher extends App {

  private val APP_NAME = "CoTweetAnalyzer"
  private val MASTER = "local"

  val session: SparkSession = init(APP_NAME, MASTER)



  def init(appName: String, master: String): SparkSession = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    SparkSession.builder().config(conf).getOrCreate()
  }




}
