package sortBigFile.db;

import sortBigFile.Record;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordsMySqlDBService implements RecordsDBService {
    public final static String TABLE_NAME = "Records";
    public final static int VARCHAR_SIZE = 50;
    private final static String CONNECTION_STRING = "jdbc:mysql://localhost:3306/records";
    private final static Object MUTEX = new Object();
    private Connection connection;
    private String[] fields;

    public RecordsMySqlDBService(String[] fields) throws SQLException {
        setFields(fields);
        Driver driver = new com.mysql.cj.jdbc.Driver();
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "linor1234");
        properties.setProperty("serverTimezone", "GMT+2");
        connection = driver.connect(CONNECTION_STRING, properties);
        dropRecords(TABLE_NAME);
        createTableIfNotExist();
    }

    private synchronized void createTableIfNotExist() throws SQLException {
        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS `");
        queryBuilder.append(TABLE_NAME);
        queryBuilder.append("` (");
        for (String field: fields) {
            queryBuilder.append(String.format(" `%s` VARCHAR(%d), ", field, VARCHAR_SIZE));
        }
        queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(","));
        queryBuilder.append(" );");
        String query = queryBuilder.toString();
        connection.createStatement().execute(query);
    }


    @Override
    public synchronized void saveRecords(Iterable<Record> records) {
        try {
            StringBuilder insertQuery = new StringBuilder("INSERT INTO `");
            insertQuery.append(TABLE_NAME);
            insertQuery.append("` (");
            insertQuery.append(String.join(", ", Stream.of(this.fields).map(field ->
                    String.format("`%s`", field)).toArray(String[]::new)));
            insertQuery.append(") VALUES (");

            insertQuery.append(String.join(", ",
                    Stream.of(fields).map(r -> "?").collect(Collectors.toList())));
            insertQuery.append(" ) ;");
            PreparedStatement statement = null;

            synchronized (MUTEX) {
                statement = connection.prepareStatement(insertQuery.toString());
//                int counter = 0;
                for (Record record : records) {
                    for (int i = 0; i < record.getValues().length; i++) {
                        statement.setString(i + 1, record.get(i));
                    }
                    statement.addBatch();
//                    counter++;
                }

                statement.executeLargeBatch();
//                System.out.println(String.format("Saved %d Records successfully, Max Batch Size: %d",
//                        counter, statement.getLargeMaxRows()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void dropRecords(String tableName) throws SQLException {
        String deleteQuery = String.format("DROP TABLE IF EXISTS %s", tableName);
        connection.createStatement().execute(deleteQuery);
    }

    public void setFields(String[] fields) {
        this.fields = Stream.of(fields).map(field -> field.replace(".", "_").trim())
                .toArray(String[]::new);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
