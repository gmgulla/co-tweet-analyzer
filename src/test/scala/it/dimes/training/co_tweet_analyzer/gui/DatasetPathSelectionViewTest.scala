package it.dimes.training.co_tweet_analyzer.gui

import org.junit.{Assert, Before, Test}

class DatasetPathSelectionViewTest {

  private var homeDirectoryPath: String = _

  @Before
  def setUp(): Unit = {
    homeDirectoryPath = "/Users/gmg/"
  }

  @Test
  def aDatasetPathWasSelected(): Unit = {
    val datasetPathSelectionView = new DatasetPathSelectionView(homeDirectoryPath)
    val pathSelected = datasetPathSelectionView.selectDatasetPath()
    println(s"Path Selected is: $pathSelected")
    Assert.assertNotNull(pathSelected)
  }

  @Test
  def noDatasetPathWasSelected(): Unit = {
    val datasetPathSelectionView = new DatasetPathSelectionView(homeDirectoryPath)
    val pathSelected = datasetPathSelectionView.selectDatasetPath()
    println(s"Path Selected is: $pathSelected")
    Assert.assertNull(pathSelected)
  }

}
