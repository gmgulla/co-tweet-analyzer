package it.dimes.training.co_tweet_analyzer

import it.dimes.training.co_tweet_analyzer.gui.{DatasetPathSelectionView, MainView}
import javax.swing.JOptionPane
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * Launches this app and provides a method to get a [[org.apache.spark.sql.SparkSession]].
 *
 * @author Gian Marco Gullà
 */
object Launcher {

  /**
   * Name for `SparkSession` configuration.
   */
  val APP_NAME = "Co Tweet Analyzer"
  /**
   * This `String` sets ''Spark'' to run in local-mode and
   * using maximum number of available threads.
   */
  val MASTER = "local[*]"

  /**
   * Initializes the `SparkSession` which runs queries.
   * It using Spark API to create a session or get already created one.
   *
   * @param appName name for the session
   * @param master defines run mode for the session
   * @return `SparkSession` which runs queries
   */
  def init(appName: String, master: String): SparkSession = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    SparkSession.builder().config(conf).getOrCreate()
  }

  /**
   * Launches this app.
   * It doesn't use any arguments from `stdin`.
   */
  def main(args: Array[String]): Unit = {
    val datasetPathSelectionView = new DatasetPathSelectionView(".")
    val path = datasetPathSelectionView.selectDatasetPath()
    if (path == null ) {
      val msg = "Must select a path to continue"
      JOptionPane.showMessageDialog(null, msg)
      System.exit(0)
    }
    new MainView(path)
  }

}
