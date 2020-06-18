package it.dimes.training.co_tweet_analyzer.gui

import java.awt.GridLayout
import java.awt.event.{ActionEvent, ActionListener}

import javax.swing._

/**
 * Implements main window which allows to select a query to run.
 *
 * @param datasetPath dataset root directory path
 */
class MainView(private val datasetPath: String) extends JFrame {

//============================================================================================||
// FIELDS and SETUP ==========================================================================||
//============================================================================================||

  // Window and main panel
  private val TITLE  = "Co-Tweet Analyzer"
  private val WIDTH = 600
  private val HEIGHT = 600
  private var panel: JPanel = _
  private var label: JLabel = _

  // Language Query Panel
  private var languageQueryPanel: JPanel = _
  private var languageQueryLabel: JLabel = _
  private var languageQueryButton: JButton = _

  // Source Query Panel
  private var sourceQueryPanel: JPanel = _
  private var sourceQueryLabel: JLabel = _
  private var sourceQueryButton: JButton = _

  // Hashtag Query Panel
  private var hashtagQueryPanel: JPanel = _
  private var hashtagQueryLabel: JLabel = _
  private var hashtagQueryButton: JButton = _

  // User Query Panel
  private var userQueryPanel: JPanel = _
  private var userQueryLabel: JLabel = _
  private var userQueryButton: JButton = _

  /**
   * Setups this window
   */
  private def setUp(): Unit = {
    val buttonsText = "Run"
    val buttonsListener = new ClickListener(this)

    // textField setup -------------------------------------------------------------------------
    val text = "Select a query to run"
    label = new JLabel(text, SwingConstants.CENTER)
    // -----------------------------------------------------------------------------------------


    // Language Query setup --------------------------------------------------------------------
    // languageQueryTextField
    val languageQueryTextFieldText = "Languages Used Query"
    languageQueryLabel = new JLabel(languageQueryTextFieldText)

    // languageQueryButton
    languageQueryButton = new JButton(buttonsText)
    languageQueryButton.addActionListener(buttonsListener)

    // languageQueryPanel
    languageQueryPanel = new JPanel()
    languageQueryPanel.add(languageQueryLabel)
    languageQueryPanel.add(languageQueryButton)
    // -----------------------------------------------------------------------------------------


    // Source Query setup ----------------------------------------------------------------------
    // sourceTextField
    val sourceQueryTextFieldText = "Sources Used Query"
    sourceQueryLabel = new JLabel(sourceQueryTextFieldText)

    // sourceQueryButton
    sourceQueryButton = new JButton(buttonsText)
    sourceQueryButton.addActionListener(buttonsListener)

    //sourceQuery Panel
    sourceQueryPanel = new JPanel()
    sourceQueryPanel.add(sourceQueryLabel)
    sourceQueryPanel.add(sourceQueryButton)
    // -----------------------------------------------------------------------------------------


    // Hashtag Query setup ---------------------------------------------------------------------
    // hashtagQueryTextField
    val hashtagQueryTextFieldText = "Hashtags Used Query"
    hashtagQueryLabel = new JLabel(hashtagQueryTextFieldText)

    // hashtagQueryButton
    hashtagQueryButton = new JButton(buttonsText)
    hashtagQueryButton.addActionListener(buttonsListener)

    //hashtagQueryPanel
    hashtagQueryPanel = new JPanel()
    hashtagQueryPanel.add(hashtagQueryLabel)
    hashtagQueryPanel.add(hashtagQueryButton)
    // -----------------------------------------------------------------------------------------

    // User Query setup ------------------------------------------------------------------------
    // userQueryTextField
    val userQueryTextFieldText = "Users Query"
    userQueryLabel = new JLabel(userQueryTextFieldText)

    // userQueryButton
    userQueryButton = new JButton(buttonsText)
    userQueryButton.addActionListener(buttonsListener)

    // userQueryPanel
    userQueryPanel = new JPanel()
    userQueryPanel.add(userQueryLabel)
    userQueryPanel.add(userQueryButton)
    // -----------------------------------------------------------------------------------------

    // Window and main panel -------------------------------------------------------------------
    // panel setup
    val numPanelRows = 5
    val numPanelCols = 1
    panel = new JPanel(new GridLayout(numPanelRows, numPanelCols))
    panel.add(label)
    panel.add(languageQueryPanel)
    panel.add(sourceQueryPanel)
    panel.add(hashtagQueryPanel)
    panel.add(userQueryPanel)


    // this view setup
    this.setTitle(TITLE)
    this.setSize(WIDTH, HEIGHT)
    this.setLocationRelativeTo(null)
    this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    this.add(panel)
    this.setVisible(true)
  }

  setUp()

//============================================================================================||
// LISTENER ==================================================================================||
//============================================================================================||

  /**
   * Defines listeners for buttons in this window
   *
   * @param mainFrame this window
   */
  private class ClickListener(private val mainFrame: JFrame) extends ActionListener {

    /**
     * Launches the view corresponding to query selected
     */
    override def actionPerformed(e: ActionEvent): Unit = {
      val source = e.getSource
      source match {
        case x if x == languageQueryButton =>
          mainFrame.setVisible(false)
          new LanguageQueryRunningView(mainFrame, datasetPath)
        case x if x == sourceQueryButton =>
          mainFrame.setVisible(false)
          new SourceQueryRunningView(mainFrame, datasetPath)
        case x if x == hashtagQueryButton =>
          mainFrame.setVisible(false)
          new HashtagQueryRunningView(mainFrame, datasetPath)
        case x if x == userQueryButton =>
          mainFrame.setVisible(false)
          new UsersQueryRunningView(mainFrame, datasetPath)
      }
    }

  }

}
