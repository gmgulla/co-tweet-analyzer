package it.dimes.training.co_tweet_analyzer.gui

import java.awt.event.{WindowAdapter, WindowEvent}

import javax.swing.{JFrame, JPanel, WindowConstants}
import org.jfree.chart.ui.ApplicationFrame
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.general.DefaultPieDataset

/**
 * Implements view to visualize query result as a `PieChart`
 * @param _title window title
 * @param dataset dataset to show
 * @param mainFrame reference to [[MainView]] instance
 */
class PieChartView(
                    _title: String,
                    private val dataset: Map[String, Long],
                    private val mainFrame: JFrame
                  ) extends ApplicationFrame(_title) {


  /**
   * Creates [[dataset]] version useful to be shown as `PieChart`
   */
  private def createPieDataset(): DefaultPieDataset = {
    val pieDataset = new DefaultPieDataset()
    val filteredDataset = dataset.filterKeys(_ != null)
    for ((name, value) <- filteredDataset) {
      pieDataset.setValue(name, value)
    }
    pieDataset
  }

  /**
   * Creates and setups the view
   */
  private def createPieChart(pieDataset: DefaultPieDataset): JFreeChart = {
    val legend = !(dataset.size > 50)
    val tooltips = true
    val urls = false
    val pieChart = ChartFactory.createPieChart(getTitle(), pieDataset, legend, tooltips, urls)
    pieChart
  }

  /**
   * Creates the [[JPanel]] for the window
   */
  private def createPanel(): JPanel = {
    val pieDataset = createPieDataset()
    val pieChart = createPieChart(pieDataset)
    new ChartPanel(pieChart)
  }

  /**
   * Shows chart window to screen
   */
  def showChart(): Unit = {
    this.setContentPane(createPanel())
    val width = 600
    val height = 400
    this.setSize(width, height)
    this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
    val windowListener = new WindowAdapter {
      override def windowClosing(e: WindowEvent): Unit = {
        mainFrame.setVisible(true)
      }
    }
    this.addWindowListener(windowListener)
    this.setVisible(true)
  }

}
