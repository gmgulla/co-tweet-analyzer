package it.dimes.training.co_tweet_analyzer.gui
import it.dimes.training.co_tweet_analyzer.Launcher
import it.dimes.training.co_tweet_analyzer.services.TweetsService
import javax.swing.{JFrame, JOptionPane}

class SourceQueryRunningView(
                              override protected val mainFrame: JFrame,
                              override protected val datasetPath: String
                            ) extends RunningView (mainFrame, datasetPath) {



  override def executeQuery(): Unit = {
    val sqlSession = Launcher.init(Launcher.APP_NAME, Launcher.MASTER)
    val tweetsService = TweetsService(sqlSession, datasetPath)
    var result: Map[String, Long] = Map()
    try {
      result = tweetsService.calculateSources()
    } catch {
      case ex: Exception =>{
        val  msg = "Something went wrong. Check that you have selected the correct path."
        this.dispose()
        JOptionPane.showMessageDialog(null, msg)
        System.exit(0)
      }
    }
    val cuttedResult = Map(result.toSeq.sortWith(_._2 > _._2):_*).take(50)
    this.dispose()
    val title = "Sources Query"
    new PieChartView(title, cuttedResult, mainFrame).showChart()
  }

}
