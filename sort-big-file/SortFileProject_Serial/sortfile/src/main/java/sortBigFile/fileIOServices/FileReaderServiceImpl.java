package sortBigFile.fileIOServices;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import sortBigFile.Record;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class FileReaderServiceImpl implements FileReaderService{
    private CSVReader csvReader;
    private String[] keys;

    public FileReaderServiceImpl(String filePath, boolean readKeys) throws IOException, CsvValidationException {
        csvReader = new CSVReader(new FileReader(filePath));
        if(readKeys){
            keys = csvReader.readNext();
        }
    }

    public FileReaderServiceImpl(String filePath) throws IOException, CsvValidationException {
        this(filePath, false);
    }

    @Override
    public Record[] readRecords(int amount) throws IOException, CsvValidationException {
        Record[] records = new Record[amount];
        for (int i = 0; i < amount; i++) {
            String[] values = csvReader.readNext();
            if(values == null) { // end of file
                return Arrays.copyOf(records, i);
            }
            Record record = new Record(values);
            records[i] = record;
        }
        return records;
    }

    @Override
    public int indexOfKey(String key) {
        validateKeys();
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) {
                return i;
            }
        }
        throw new IllegalArgumentException("The key: '" + key + "' is not exist");
    }

    @Override
    public String[] getKeys() {
        validateKeys();
        return keys;
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }

    private void validateKeys(){
        if(keys == null) {
            throw new IllegalAccessError("Keys was not read, use readKey parameter in the constructor");
        }
    }
}
