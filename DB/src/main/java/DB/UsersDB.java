package DB;

import DB.Tables.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Provides methods to work with the Users database
 */
public class UsersDB {
    private final static String CSV_PATH = "./csv_data/";
    private final static String CHARTS_PATH = "./charts/";
    private final static String EXCEL_PATH = "./query_results/";
    private final static String db_PATH = "./db/";
    private final static String QUERIES_PATH = "./src/main/sqlQueries/";
    private final static String DATA_SOURCE1  = "https://drive.google.com/uc?id=1ifEvF8w2z9InLelvGmoiAiZg2LJ6K1me";
    private final static String DATA_SOURCE2 = "https://drive.google.com/uc?id=1kjF7z-eEU6kP8er_BRGC_I9JXb4e2xNu";


    private List<BaseTable> tables;

    public static String getQueriesPath(){
        return QUERIES_PATH;
    }

    public UsersDB(){
        try {
            tables = new ArrayList<>(Arrays.asList(new UserLogsDB(), new UserDataDB()));
            File csvFolder = new File(CSV_PATH);
            File chartsFolder = new File(CHARTS_PATH);
            File excelFolder = new File(EXCEL_PATH);
            File dbFolder = new File(db_PATH);
            if (csvFolder.exists()) {
                deleteFile(csvFolder);
            }
            if(chartsFolder.exists()){
                deleteFile(chartsFolder);
            }
            if(excelFolder.exists()){
                deleteFile(excelFolder);
            }
            if(dbFolder.exists()){
                deleteFile(dbFolder);
            }
            assert csvFolder.mkdir();
            assert chartsFolder.mkdir();
            assert excelFolder.mkdir();
            assert dbFolder.mkdir();
            downloadFile(DATA_SOURCE1, "UserLogs.txt");
            downloadFile(DATA_SOURCE2, "UserData.txt");
            createTables();
            loadTablesFromCsv();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            UsersDBConnection.closeConnection();
        }
    }

    public void createTables() {
        for (BaseTable table : tables) {
            table.createTable();
        }
    }

    private void loadTablesFromCsv() {
        for (BaseTable table : tables) {
            table.insertDataFromCsv(CSV_PATH + table.getTableName() + ".txt");
        }
    }

    private void downloadFile(String file_url, String fileName) throws IOException {
        URL url = new URL(file_url);
        URLConnection connection = url.openConnection();
        InputStream inputStream = connection.getInputStream();
        Path path = new File(UsersDB.CSV_PATH + fileName).toPath();
        Files.copy(inputStream, path);
    }

    public static boolean deleteFile(File fileToDelete) {
        File[] allContents = fileToDelete.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteFile(file);
            }
        }
        return fileToDelete.delete();
    }

}
