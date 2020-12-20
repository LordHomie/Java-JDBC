package DB;

import java.awt.Font;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates charts representing the results of some QueryRunner queries
 */
public class UsersDBChartBuilder {
    private final QueryRunner queryRunner;
    private static final int chartWidth = 1920;
    private static final int chartHeight = 1080;

    public UsersDBChartBuilder(QueryRunner runner) {
        queryRunner = runner;
    }

    /**
     * Converts List of strings to JFreeChart dataset
     * @param data List of String.
     *             List[0] -- numeric values for Y axis
     *             List[1] -- string values for X axis
     * @return JFreeChart dataset
     */
    private DefaultCategoryDataset fillDataset(List<String[]> data) {
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (String[] row : data) {
            dataset.addValue(Double.parseDouble(row[1]), "default", row[0]);
        }
        return dataset;
    }

    /**
     * Apply default settings to the chart and set up title
     * @param barChart chart to set up
     * @param title title
     */
    private void setupChart(JFreeChart barChart, String title) {
        CategoryPlot plot = barChart.getCategoryPlot();
        CategoryAxis axis = plot.getDomainAxis();

        Font font = new Font("Cambria", Font.BOLD, 25);
        axis.setTickLabelFont(font);
        Font font3 = new Font("Cambria", Font.BOLD, 30);
        barChart.setTitle(new org.jfree.chart.title.TextTitle(title, new java.awt.Font("Cambria", java.awt.Font.BOLD, 40)));

        plot.getDomainAxis().setLabelFont(font3);
        plot.getRangeAxis().setLabelFont(font3);
        CategoryPlot categoryPlot = (CategoryPlot) barChart.getPlot();
        BarRenderer renderer = (BarRenderer) categoryPlot.getRenderer();
        renderer.setBarPainter(new StandardBarPainter());
    }

    /**
     * Creates a chart, representing the number most occurrent users
     * @param filepath where to store the chart
     * @throws IOException if a problem while saving the chart to the filesystem occurred
     */
    public void query1CreateBarChart(String filepath, int N_Users) throws IOException {
        ArrayList<String[]> data = queryRunner.topN_most_active_users(N_Users);
        DefaultCategoryDataset dataset = fillDataset(data);
        String title = "Top-N most active users";
        String categoryAxis = "User_IP";
        String valueAxis = "Occurrence";

        JFreeChart barChart = ChartFactory.createBarChart(title, categoryAxis, valueAxis, dataset,
                PlotOrientation.VERTICAL, false, false, false);
        setupChart(barChart, title);
        ChartUtilities.saveChartAsPNG(new File(filepath), barChart, chartWidth, chartHeight);
    }

    /**
     * Creates a chart, representing the number of queries for a given day.
     * @param filepath where to store the chart
     * @throws IOException if a problem while saving the chart to the filesystem occurred
     */
    public void query2CreateBarChart(String filepath, int day) throws IOException {
        ArrayList<String[]> data = queryRunner.QueriesNumber(day);
        DefaultCategoryDataset dataset = fillDataset(data);
        String title = "number of queries for a given day";
        String categoryAxis = "User_IP";
        String valueAxis = "day"; //this doesn't exist which made me to not be able to create the pie chart :(

        JFreeChart barChart = ChartFactory.createBarChart(title, categoryAxis, valueAxis, dataset,
                PlotOrientation.VERTICAL, false, false, false);
        setupChart(barChart, title);
        ChartUtilities.saveChartAsPNG(new File(filepath), barChart, chartWidth, chartHeight);
    }

    /**
     * Creates a chart, representing the three most popular websites.
     * @param filepath where to store the chart
     * @throws IOException if a problem while saving the chart to the filesystem occurred
     */
    public void query3CreateBarChart(String filepath, int ageLimit, int lastDays) throws IOException {
        ArrayList<String[]> data = queryRunner.ThreeMostPopularWebsites(ageLimit, lastDays);
        DefaultCategoryDataset dataset = fillDataset(data);
        String title = "The three most popular websites";
        String categoryAxis = "Website";
        String valueAxis = "IP";
        JFreeChart barChart = ChartFactory.createBarChart(title, categoryAxis, valueAxis, dataset,
                PlotOrientation.VERTICAL, false, false, false);
        setupChart(barChart, title);
        ChartUtilities.saveChartAsPNG(new File(filepath), barChart, chartWidth, chartHeight);
    }
}
