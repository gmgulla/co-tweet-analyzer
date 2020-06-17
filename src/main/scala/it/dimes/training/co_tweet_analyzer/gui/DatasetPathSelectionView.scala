package it.dimes.training.co_tweet_analyzer.gui

import javax.swing.{JFileChooser, JOptionPane}

class DatasetPathSelectionView (private val homeDirectoryPath: String)  {

//============================================================================================||
// FIELDS and SETUP ==========================================================================||
//============================================================================================||


  private val TITLE: String = "Choose Dataset Directory"
  private var chooser: JFileChooser = _


  private def setUp(): Unit = {

    chooser = new JFileChooser(homeDirectoryPath)
    chooser.setDialogTitle(TITLE)
    chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY)
    val areAllFilesAccepted = false
    chooser.setAcceptAllFileFilterUsed(areAllFilesAccepted)
  }

  setUp()

//============================================================================================||
// METHODS ===================================================================================||
//============================================================================================||

  def selectDatasetPath(): String = {
    val selectionState = chooser.showDialog(null, "OK")
    if (selectionState == JFileChooser.APPROVE_OPTION) {
      return chooser.getSelectedFile().getAbsolutePath
    }
    null
  }

}

