package DB.Tables;

import DB.UsersDBConnection;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;


public class UserLogsDB extends BaseTable{
    public UserLogsDB(){
        super("UserLogs");
    }

    public void createTable() {
        File createTableQuery = new File("./src/main/sqlQueries/UserLogsCreateTable.sql");
        if (!UsersDBConnection.executeSqlUpdateQueryFromFile(createTableQuery)){
            logger.log(Level.SEVERE ,"Failed to create UserLogs table");
        } else{
            logger.info("Created UserLogs table");
        }
    }

    public PreparedStatement getStatement() throws SQLException {
        String query = String.format("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)", tableName);
        return UsersDBConnection.getConnection().prepareStatement(query);
    }

    public void addInsertionToBatch(PreparedStatement preparedStatement, String[] values){
        try {
            preparedStatement.setString(1, values[0]);
            Date original = new SimpleDateFormat("yyyyMMddHHmmss").parse(values[1]);
            String newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(original);
            preparedStatement.setTimestamp(2, Timestamp.valueOf(newFormat));

            preparedStatement.setString(3, values[2]);
            preparedStatement.setInt(4, Integer.parseInt(values[3]));
            preparedStatement.setInt(5, Integer.parseInt(values[4]));

            String[] split = values[5].split(" ");
            preparedStatement.setString(6, split[1]);
            preparedStatement.addBatch();
        } catch (SQLException | ParseException e) {
            logger.log(Level.SEVERE , Arrays.toString(e.getStackTrace()));
        }
    }
}
