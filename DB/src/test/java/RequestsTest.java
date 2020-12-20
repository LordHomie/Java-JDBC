import DB.QueryRunner;
import DB.UsersDB;
import java.io.IOException;
import java.util.ArrayList;
import DB.UsersDBChartBuilder;
import DB.UsersDBXlsxTableBuilder;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class RequestsTest {
    static QueryRunner queryRunner;

    @BeforeAll
    static void setup()   {
        queryRunner = new QueryRunner(new UsersDB());
        System.out.println("DB is ready");
    }


    @Test
    void queryB1Test() {
        ArrayList<String[]> result = queryRunner.topN_most_active_users(20);
        assertNotNull(result);
        System.out.println("Find top-N most active users:");
        for (String[] row : result) {
            System.out.println(row[3] + "  " + row[2] + "  Age: " + row[0] + "  Occurrence: " + row[1]);
        }
    }

    @Test
    void queryB2Test() {
        ArrayList<String[]> result = queryRunner.QueriesNumber(20140212);
        assertNotNull(result);
        System.out.println("Number of queries a given day:");
        for (String[] row : result) {
            System.out.println(row[0]);
        }
    }

    @Test
    void queryB3Test() {
        ArrayList<String[]> result = queryRunner.ThreeMostPopularWebsites(27,2);
        assertNotNull(result);
        System.out.println("the three most popular websites are:");
        for (String[] row : result) {
            System.out.println(row[0] + "  " + row[1]);
        }
    }

    @Test
    void queryB4Test() {
        boolean result = queryRunner.add_newEntry("22.493.14.999",
                "20140212111111",
                "http://youtube.com/c/BabushkaBoyz",
                1900,
                500,
                "Microsoft Edge/5.0 (Windows; U; MSIE 9.0; Windows NT 8.1; Trident/5.0; .NET4.0E; en-AU)n"
        );
        if (result) {
            System.out.println("New Entry added");
        } else {
            System.out.println("New Entry not added");
            assert false;
        }
    }

    @Test
    void queryB5Test() {
        int result = queryRunner.DeleteEntries("20140426191049");
        System.out.println(String.format("Deleted %d rows\n", result));
    }

//    @Test
    void ShowUserLogs() {queryRunner.ShowUserLogs();}

    @Test
    void getAllTables() {
        try {
            UsersDBXlsxTableBuilder tableBuilder = new UsersDBXlsxTableBuilder(queryRunner);
            tableBuilder.b1CreateTable(20);
            tableBuilder.b2CreateTable(20140212);
            tableBuilder.b3CreateTable(27,2);
        } catch (IOException e) {
            assert false;
        }
    }
//
    @Test
    void buildAllCharts() {
        final String OUTPUT_PNG_PATH = "./charts/";
        try {
            UsersDBChartBuilder chartBuilder = new UsersDBChartBuilder(queryRunner);
            chartBuilder.query1CreateBarChart(OUTPUT_PNG_PATH + "query1BarChart.png", 20);
//            chartBuilder.query2CreateBarChart(OUTPUT_PNG_PATH + "query2BarChart.png", 20140425);
            chartBuilder.query3CreateBarChart(OUTPUT_PNG_PATH + "query3BarChart.png", 27, 2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}