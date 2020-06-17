package it.dimes.training.co_tweet_analyzer.gui

import org.junit.{Assert, Before, Test}

class MainViewTest {

  private var datasetPath: String = _

  @Before
  def setUp(): Unit = {
    datasetPath = "/Users/gmg/"
  }

  @Test
  def isSetUpCorrect(): Unit = {
    val mainView = new MainView(datasetPath)
    Assert.assertNotNull(mainView)
  }


}
