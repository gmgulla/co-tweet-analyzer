package it.dimes.training.co_tweet_analyzer.gui

import javax.swing.JFileChooser

/**
 * Implements opening UI to allow selecting dataset root directory hierarchy path.
 *
 * @param homeDirectoryPath path where open selection window
 *
 * @author Gian Marco Gullà
 */
class DatasetPathSelectionView (private val homeDirectoryPath: String)  {

//============================================================================================||
// FIELDS and SETUP ==========================================================================||
//============================================================================================||


  private val TITLE: String = "Choose Dataset Directory"
  private var chooser: JFileChooser = _


  /**
   * Setups UI
   */
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

  /**
   * Launch windows to select dataset root directory hierarchy path
   * and return it
   *
   * @return dataset root directory path
   */
  def selectDatasetPath(): String = {
    val selectionState = chooser.showDialog(null, "OK")
    if (selectionState == JFileChooser.APPROVE_OPTION) {
      return chooser.getSelectedFile().getAbsolutePath
    }
    null
  }

}

