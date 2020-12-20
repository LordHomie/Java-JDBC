package DB.Tables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import DB.UsersDBConnection;

public abstract class BaseTable {
    protected String tableName;
    static Logger logger;

    static {
        logger = Logger.getLogger(BaseTable.class.getName());
    }

    BaseTable(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public abstract void createTable();

    public abstract void addInsertionToBatch(PreparedStatement preparedStatement, String[] values) throws SQLException;

    public abstract PreparedStatement getStatement() throws SQLException;

    public void insertDataFromCsv(String filePath) {
        try (BufferedReader file = new BufferedReader(new FileReader(filePath))) {
            PreparedStatement preparedStatement = getStatement();
            String line;
            String[] new_line;
            while ((line = file.readLine()) != null) {
                new_line = line.split("\\t+");
                addInsertionToBatch(preparedStatement, new_line);
            }
            file.close();
            int[] rows = preparedStatement.executeBatch();
            UsersDBConnection.getConnection().commit();
            int addedLinesNumber = Arrays.stream(rows).sum();
            logger.log(Level.INFO, String.format("Added %d lines to %s", addedLinesNumber, tableName));

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (FileNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to read csv file");
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }
}

