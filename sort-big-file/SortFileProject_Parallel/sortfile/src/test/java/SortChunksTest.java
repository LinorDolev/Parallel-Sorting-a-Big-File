import com.opencsv.exceptions.CsvValidationException;
import org.junit.Test;
import sortBigFile.Algorithm;
import sortBigFile.fileIOServices.FileReaderService;
import sortBigFile.fileIOServices.FileReaderServiceImpl;

import java.io.IOException;
import java.util.Date;

public class SortChunksTest {

    @Test
    public void testChunks() throws IOException, CsvValidationException {
        final String FILE_PATH = ".\\data\\24 Records.csv";
        final String RESULT_PATH = String.format(".\\data\\SortedRecords%d.csv", new Date().getTime());

        final int SIZE = 24;
        final int MEM_SIZE = 6;
        final String SORT_KEY = "Region";
        System.out.println("TestChunks: ");
        double t0 = System.currentTimeMillis();
        Algorithm.sortBigFile(FILE_PATH, SIZE, MEM_SIZE, SORT_KEY, RESULT_PATH);
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - t0) / 1000.0);
        FileReaderService fileReaderService = new FileReaderServiceImpl(RESULT_PATH, true);
        assert fileReaderService.readRecords(SIZE).length == SIZE;
    }

    @Test
    public void testMemorySizeNotDivisibleByNumOfChunks() throws IOException, CsvValidationException{
        final String FILE_PATH = ".\\data\\10000 Records.csv";
        final String RESULT_PATH = String.format(".\\data\\SortedRecords%d.csv", new Date().getTime());

        final int SIZE = 10000;
        final int MEM_SIZE = 101;
        final String SORT_KEY = "First Name";
        System.out.println("TestMemorySizeNotDivisibleByNumOfChunks: ");
        double t0 = System.currentTimeMillis();
        Algorithm.sortBigFile(FILE_PATH, SIZE, MEM_SIZE, SORT_KEY, RESULT_PATH);
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - t0) / 1000.0);
        FileReaderService fileReaderService = new FileReaderServiceImpl(RESULT_PATH, true);
        assert fileReaderService.readRecords(SIZE).length == SIZE;
    }

    @Test
    public void testMemoryBiggerThanFile() throws IOException, CsvValidationException {
        final String FILE_PATH = ".\\data\\10000 Records.csv";
        final String RESULT_PATH = String.format(".\\data\\SortedRecords%d.csv", new Date().getTime());

        final int SIZE = 10000;
        final int MEM_SIZE = 100000;
        final String SORT_KEY = "First Name";
        System.out.println("TestMemoryBiggerThanFile: ");
        double t0 = System.currentTimeMillis();
        Algorithm.sortBigFile(FILE_PATH, SIZE, MEM_SIZE, SORT_KEY, RESULT_PATH);
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - t0) / 1000.0);
        FileReaderService fileReaderService = new FileReaderServiceImpl(RESULT_PATH, true);
        assert fileReaderService.readRecords(SIZE).length == SIZE;
    }

    /**
     *  This causing: memory < numOfChunks
     *  Not Working!
     *  TODO Handle this case using partition merging
     */

    @Test
    public void testMemoryIsSmallerThanSqrtOfFileSize() throws IOException, CsvValidationException {
        final String FILE_PATH = ".\\data\\10000 Records.csv";
        final String RESULT_PATH = String.format(".\\data\\SortedRecords%d.csv", new Date().getTime());

        final int SIZE = 10000;
        final int MEM_SIZE = 10;
        final String SORT_KEY = "First Name";
        Algorithm.sortBigFile(FILE_PATH, SIZE, MEM_SIZE, SORT_KEY, RESULT_PATH);
        FileReaderService fileReaderService = new FileReaderServiceImpl(RESULT_PATH, true);
        assert fileReaderService.readRecords(SIZE).length == SIZE;
    }

}
