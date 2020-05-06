package it.dimes.training.co_tweet_analyzer.gui

import javax.swing.JPanel
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.ui.ApplicationFrame
import org.jfree.data.general.DefaultPieDataset

import scala.collection.mutable

class PieChartGUI (_title: String, _dataset: mutable.HashMap[String, Long]) extends ApplicationFrame(_title) {

  private val dataset: mutable.HashMap[String, Long] = _dataset

  private def createPieDataset(): DefaultPieDataset = {
    val pieDataset = new DefaultPieDataset()
    for ((name, value) <- dataset) {
      pieDataset.setValue(name, value)
    }
    pieDataset
  }

  private def createPieChart(pieDataset: DefaultPieDataset): JFreeChart = {
    val legend = true
    val tooltips = true
    val urls = false
    val pieChart = ChartFactory.createPieChart(getTitle(), pieDataset, legend, tooltips, urls)
    pieChart
  }

  private def createPanel(): JPanel = {
    val pieDataset = createPieDataset()
    val pieChart = createPieChart(pieDataset)
    new ChartPanel(pieChart)
  }

  def showChart(): Unit = {
    this.setContentPane(createPanel())
    val width = 600
    val height = 400
    this.setSize(width, height)
    this.setVisible(true)
  }


}
