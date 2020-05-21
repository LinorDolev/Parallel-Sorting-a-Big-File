import org.junit.Before;
import org.junit.Test;
import sortBigFile.db.RecordsDBService;
import sortBigFile.db.RecordsMySqlDBService;

import java.sql.SQLException;

public class TestDBService {

    @Test
    public void testCreateTable() throws SQLException {
        RecordsDBService recordsDBService = new RecordsMySqlDBService(new String[]{"A", "B", "C", "D E"});
    }
}
