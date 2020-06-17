package it.dimes.training.co_tweet_analyzer

import it.dimes.training.co_tweet_analyzer.gui.{DatasetPathSelectionView, MainView}
import javax.swing.JOptionPane
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Launcher {

  val APP_NAME = "CoTweetAnalyzer"
  val MASTER = "local[*]"

  def init(appName: String, master: String): SparkSession = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    SparkSession.builder().config(conf).getOrCreate()
  }

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
