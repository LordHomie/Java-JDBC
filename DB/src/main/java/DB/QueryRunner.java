package DB;

import java.sql.*;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs queries from the task
 */
public class QueryRunner {
    private final static String QUERIES_PATH = UsersDB.getQueriesPath();
    static Logger logger;
    private final UsersDB usersdb;
    static{
        logger = Logger.getLogger(QueryRunner.class.getName());
    }

    public QueryRunner(UsersDB db){
        usersdb = db;
    }

    /**
     * @param N_Users - number of most active users
     * @return List of N_Users User_IP with greatest number occurrence in descending order
     * String[0] -- User_IP, String[1] -- number of occurrence
     */

    public ArrayList<String[]> topN_most_active_users(int N_Users) {
        ArrayList<String[]> returnValue = new ArrayList<>();
        try {
        File file = new File("./src/main/sqlQueries/task_B/B1.sql");
        String query = UsersDBConnection.parseQueryFromFile(file)[0];
        PreparedStatement preparedStatement = UsersDBConnection.getConnection().prepareStatement(query);
        preparedStatement.setInt(1, N_Users);
        ResultSet queryResult = preparedStatement.executeQuery();
        queryResult.beforeFirst();
        while (queryResult.next()) {
            String[] result = new String[4];
            result[0] = queryResult.getString("Age");
            result[1] = queryResult.getString("Occurrence");
            result[2] = queryResult.getString("Gender");
            result[3] = queryResult.getString("User_IP");
            returnValue.add(result);
        }
            queryResult.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    /**
     * @param day - number of queries for a given day.
     * We explained our issue in the pull request on Gitlab since we think that there's a mistake in the question statement.
     * @return number of queries for a given day.
     * String[0] -- queries_number
     */
    public ArrayList<String[]> QueriesNumber(int day) {
        ArrayList<String[]> returnValue = new ArrayList<>();
        try {
            File file = new File("./src/main/sqlQueries/task_B/B2.sql");
            String query = UsersDBConnection.parseQueryFromFile(file)[0];
            PreparedStatement preparedStatement = UsersDBConnection.getConnection().prepareStatement(query);
            preparedStatement.setInt(1, day);
            ResultSet queryResult = preparedStatement.executeQuery();
            queryResult.beforeFirst();
            while (queryResult.next()) {
                String[] result = new String[1];
                result[0] = queryResult.getString("queries_number");
                returnValue.add(result);
            }
            queryResult.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    /**
     * @param ageLimit - users under age A.
     * @param lastDays - for the B days.
     * @return the three most popular websites.
     * String[0] -- Websites, String[1] -- IP addresses
     */
    public ArrayList<String[]> ThreeMostPopularWebsites (int ageLimit, int lastDays) {
        ArrayList<String[]> returnValue = new ArrayList<>();
        try {
            File file = new File("./src/main/sqlQueries/task_B/B3.sql");
            String query = UsersDBConnection.parseQueryFromFile(file)[0];
            PreparedStatement preparedStatement = UsersDBConnection.getConnection().prepareStatement(query);
            preparedStatement.setInt(1, ageLimit);
            preparedStatement.setInt(2, lastDays);
            ResultSet queryResult = preparedStatement.executeQuery();
            queryResult.beforeFirst();
            while (queryResult.next()) {
                String[] result = new String[2];
                result[0] = queryResult.getString("Website");
                result[1] = queryResult.getString("IP");
                returnValue.add(result);
            }
            queryResult.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    /**
     * Adds a new entry to database.
     *
     * @return true if new entry was added, false if wasn't.
     */
    public boolean add_newEntry(String User_IP,
                             String TimeStamp,
                             String HTTP_query,
                             Integer WebPageSize,
                             Integer HTTP_StatusCode,
                             String Information) {


        try {

            File file = new File(QUERIES_PATH + "task_B/B4.sql");
            String[] query = UsersDBConnection.parseQueryFromFile(file);

            PreparedStatement addEntries = UsersDBConnection.getConnection().prepareStatement(query[0]);
            addEntries.setString(1, User_IP);

            Date original = new SimpleDateFormat("yyyyMMddHHmmss").parse(String.valueOf(TimeStamp));
            String newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(original);
            addEntries.setTimestamp(2, Timestamp.valueOf(newFormat));

            addEntries.setString(3, HTTP_query);
            addEntries.setInt(4, WebPageSize);
            addEntries.setInt(5, HTTP_StatusCode);
            addEntries.setString(6, Information);
            addEntries.executeUpdate();

            UsersDBConnection.getConnection().commit();
        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Level.WARNING, "New Entry was not added");
            return false;
        }
        return true;
    }

    /**
     * Deletes all the flights (in a cascade way) of aircraft of selected model
     *
     * @return number of deleted rows
     */
    public int DeleteEntries(String day) {
        try {
            File file = new File(QUERIES_PATH + "task_B/B5.sql");
            String query = UsersDBConnection.parseQueryFromFile(file)[0];
            PreparedStatement preparedStatement = UsersDBConnection.getConnection().prepareStatement(query);
            Date original = new SimpleDateFormat("yyyyMMddHHmmss").parse(String.valueOf(day));
            String newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(original);
            preparedStatement.setString(1, newFormat);
            return preparedStatement.executeUpdate();
        } catch (IOException | SQLException | ParseException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public ArrayList<String[]> ShowUserLogs() {
        try {

        File file = new File(QUERIES_PATH + "task_B/ShowUserLogs.sql");
        String[] query = UsersDBConnection.parseQueryFromFile(file);
        PreparedStatement checkData = UsersDBConnection.getConnection().prepareStatement(query[0]);

        ResultSet rs = checkData.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = rs.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
        }
        System.out.println("");
        }

        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Level.WARNING, "Not be able to show values");
        }
        return null;
    }
}
