package it.dimes.training.co_tweet_analyzer

import org.junit.{Before, BeforeClass, Test}
import org.junit.Assert._


class LauncherTest {

  var launcher: Launcher.type = _
  var appName: String = _
  var master: String = _


  @Before
  def setUp(): Unit = {
    launcher = Launcher
    appName = "Launcher Test"
    master = "local"
  }

  @Test
  def isCorrectLaunch: Unit = {
    val sparkSession = launcher.init(appName, master)
    assertNotNull("Spark Context was not created", sparkSession)
  }


}
