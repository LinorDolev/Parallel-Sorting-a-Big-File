package sortBigFile.db;


import sortBigFile.Record;
import java.io.Closeable;
import java.sql.SQLException;


public interface RecordsDBService extends Closeable {

    void saveRecords(Iterable<Record> records);

    void dropRecords(String tableName) throws SQLException;
}
