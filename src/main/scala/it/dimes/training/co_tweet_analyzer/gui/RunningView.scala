package it.dimes.training.co_tweet_analyzer.gui

import java.awt.FlowLayout
import java.awt.event.{WindowAdapter, WindowEvent}

import javax.swing._


/**
 * Defines common base for ''waiting view'' which is visible during a query running.
 *
 * @param mainFrame [[MainView]] instance
 * @param datasetPath dataset root directory path
 *
 * @author Gian Marco Gullà
 */
abstract class RunningView(
                   protected val mainFrame: JFrame,
                   protected val datasetPath: String
                 ) extends JFrame  {

  private val WIDTH = 1000
  private val HEIGHT = 100
  private var panel: JPanel = _
  private var textLabel: JLabel = _

//============================================================================================||
// FIELDS and SETUP ==========================================================================||
//============================================================================================||

  /**
   * Setups this view
   */
  private def setUp(): Unit = {
    val text = s"Query is running.\nIt may take several minutes to get the result, depending on the complexity of the query and the computing power of your PC.\nPlease wait ..."
    textLabel = new JLabel(text)
    val x = 50
    val y = 100
    val labelWidth = 250
    val labelHeight = 20
    textLabel.setBounds(x,y, labelWidth,labelHeight)

    panel = new JPanel(new FlowLayout(FlowLayout.LEADING))
    panel.add(textLabel)

    val windowListener = new WindowAdapter {
      override def windowClosing(e: WindowEvent): Unit = mainFrame.setVisible(true)
    }
    this.setSize(WIDTH, HEIGHT)
    this.setLocationRelativeTo(null)
    this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
    this.add(panel)
    this.addWindowListener(windowListener)
    this.setVisible(true)
    JOptionPane.showMessageDialog(null, "Please, click Ok and wait...", "Query is running", JOptionPane.PLAIN_MESSAGE)

  }

  /**
   * Executes query selected in `MainView`
   */
  protected def executeQuery(): Unit

//============================================================================================||
// SCRIPT ====================================================================================||
//============================================================================================||

  setUp()
  executeQuery()

}
