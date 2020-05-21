package sortBigFile.fileIOServices;

import com.opencsv.exceptions.CsvValidationException;

import java.io.Closeable;
import java.io.IOException;

import sortBigFile.Record;

public interface FileReaderService extends Closeable {
    Record[] readRecords(int amount) throws IOException, CsvValidationException;

    int indexOfKey(String key);

    String[] getKeys();
}
