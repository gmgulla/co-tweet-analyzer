package it.dimes.training.co_tweet_analyzer

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, BeforeClass, Test}
import org.junit.Assert._


class LauncherTest {

  var launcher: Launcher.type = _
  var appName: String = _
  var master: String = _
  var partitions: String = _
  var session: SparkSession = _


  @Before
  def setUp(): Unit = {
    launcher = Launcher
    appName = "Launcher Test"
    master = "local"
    partitions = "*"
  }


  @Test
  def isCorrectInit: Unit = {
    val session1 = launcher.init(appName, s"$master[$partitions]")
    assertNotNull("Spark Context was not created", session1)
    val session2 = launcher.init(appName, s"$master[$partitions]")
    assertEquals(session1, session2)
  }


}