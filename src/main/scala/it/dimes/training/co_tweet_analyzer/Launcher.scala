package it.dimes.training.co_tweet_analyzer

import org.apache.parquet.format.UUIDType
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrameReader, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Launcher  extends App {

  val resPath = "src/main/resources/data/"

  // Init Spark Context
  val conf = new SparkConf()
  conf.setAppName("CoTweetAnalyzer")
  conf.setMaster("local")
  val spark = new SparkContext(conf)

  // Init SQL Spark Session
  val session = SparkSession.builder().getOrCreate()


  val tweetsPath = "coronavirus-covid19-tweets/"
  val countriesPath = "other/Countries.CSV"
  val hashtagsPath = "other/Hashtags.CSV"

  val tweetsSchema = new StructType()
    .add("tweetId", LongType, true)
    .add("userId", LongType, true)
    .add("tweetDate", DateType, true)
    .add("userName", StringType, true)
    .add("text", StringType, true)
    .add("source", StringType, true)
    .add("replyToTweetId", LongType, true)
    .add("replyToUserId", LongType, true)
    .add("replyToUserName", StringType, true)
    .add("isQuote", BooleanType, true)
    .add("isRetweet", BooleanType, true)
    .add("likeCount", IntegerType, true)
    .add("retweetCount", IntegerType, true)
    .add("countryCode", StringType, true)
    .add("placeFullName", StringType, true)
    .add("placeType", StringType, true)
    .add("followerCount", IntegerType, true)
    .add("friendsCount", IntegerType, true)
    .add("accountLang", StringType, true)
    .add("accountCreationDate", DateType, true)
    .add("isVerified", BooleanType, true)
    .add("tweetLang", StringType, true)

  val tweets = session.read.option("header", "true").schema(tweetsSchema).csv(s"${Launcher.resPath}/$tweetsPath")

  tweets.printSchema()
  tweets.show()










}
