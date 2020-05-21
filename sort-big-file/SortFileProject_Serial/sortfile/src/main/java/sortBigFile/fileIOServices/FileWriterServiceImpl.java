package sortBigFile.fileIOServices;

import com.opencsv.CSVWriter;
import sortBigFile.Record;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Queue;

public class FileWriterServiceImpl implements FileWriterService {
    private CSVWriter csvWriter;

    public FileWriterServiceImpl(String filePath) throws IOException {
        csvWriter = new CSVWriter(new FileWriter(filePath));
    }

    @Override
    public void writeRecords(Queue<Record> records) {
        while (!records.isEmpty()){
            csvWriter.writeNext(records.poll().getValues(), false);
        }
        try {
            csvWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(String[] record){
        csvWriter.writeNext(record, false);
    }

    public static void writeRecordsToANewFile(String newFilePath, Record[] records) throws IOException {
        CSVWriter newFileWriter = new CSVWriter(new FileWriter(newFilePath));
        for (Record record: records) {
            newFileWriter.writeNext(record.getValues());
        }
        newFileWriter.close();
    }

    @Override
    public void close() throws IOException {
        csvWriter.close();
    }
}
