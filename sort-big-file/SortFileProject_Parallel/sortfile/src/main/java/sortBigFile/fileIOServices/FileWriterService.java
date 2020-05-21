package sortBigFile.fileIOServices;

import java.io.Closeable;
import java.util.Queue;

import sortBigFile.Record;

public interface FileWriterService extends Closeable {
    void writeRecords(Queue<Record> records);

    void writeRecord(String[] record);
}
