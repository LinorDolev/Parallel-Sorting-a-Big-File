package sortBigFile;

import com.opencsv.exceptions.CsvValidationException;
import sortBigFile.db.RecordsDBService;
import sortBigFile.db.RecordsMySqlDBService;
import sortBigFile.fileIOServices.FileReaderService;
import sortBigFile.fileIOServices.FileReaderServiceImpl;
import sortBigFile.fileIOServices.FileWriterService;
import sortBigFile.fileIOServices.FileWriterServiceImpl;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Algorithm {
    private static final String TEMP_FILE_NAME_FORMAT = ".\\SORTED_CHUNK_%d.csv";
    private static final int NUM_THREADS = 4;

    /**
     * sortBigFile.Main function
     *
     * @param filePath   - path to big file to sort
     * @param resultPath - path to write the sorted array to.
     *                   <p>
     *                   Steps:
     *                   1. Read maxMemory sized chunks - once at a time - sort them and write them back to a file
     *                   2. Read maxMemory/numberOfChunks smallest records from each sorted chunk
     *                   3. Merge those records into one buffer (his size is maxMemory)
     *                   4. Write into a new file the maxMemory/numberOfChunks smallest records
     *                   5. Read the next maxMemory/(numberOfChunks)
     *                   6. Go back to 3 until there are no records left in any of the files.
     */
    public static void sortBigFile(String filePath, int fileSize, int maxMemory, String sortKey, String resultPath) {
        int remainingChunk = fileSize % maxMemory == 0 ? 0 : 1;
        int numberOfChunks = (fileSize / maxMemory) + remainingChunk;
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        try (FileReaderService fileReader = new FileReaderServiceImpl(filePath, true);
             FileWriterService resultWriter = new FileWriterServiceImpl(resultPath);
             RecordsDBService recordsDBService = new RecordsMySqlDBService(fileReader.getKeys())) {
            int sortKeyIndex = fileReader.indexOfKey(sortKey);
            resultWriter.writeRecord(fileReader.getKeys());
            Comparator<Record> comparator = Comparator.comparing(r -> r.get(sortKeyIndex));
            sortChunks(fileReader, numberOfChunks, maxMemory, comparator, executorService);
            mergeChunks(maxMemory, numberOfChunks, resultWriter, comparator, executorService,
                        recordsDBService);

        }catch (CsvValidationException | IOException | InterruptedException | SQLException e) {
            e.printStackTrace();
        }
        finally {
            executorService.shutdown();
        }
    }

    /**
     * @param fileReader     - reader of the original big file that we want to sort
     * @param numberOfChunks - number of records in this file / max memory size
     * @param maxMemory      - max number of records we are allowed to store in memory
     */
    private static void sortChunks(FileReaderService fileReader, int numberOfChunks, int maxMemory,
                                   Comparator<Record> comparator, ExecutorService executorService) throws IOException, CsvValidationException, InterruptedException {
        for (int i = 0; i < numberOfChunks; i++) {
            Record[] records = fileReader.readRecords(maxMemory);
            ParallelAlgorithms.parallelMergeSort(records, comparator, executorService);
            String fileName = String.format(TEMP_FILE_NAME_FORMAT, i);
            File file = new File(fileName);
            file.deleteOnExit();
            FileWriterServiceImpl.writeRecordsToANewFile(file.getAbsolutePath(), records);
        }
    }

    private static void mergeChunks(int maxMemory, int numberOfChunks, FileWriterService resultWriter,
                                    Comparator<Record> comparator, ExecutorService executorService,
                                    RecordsDBService recordsDBService) throws IOException, CsvValidationException, SQLException {
        boolean mergeFinished;
        int queueMaxSize = maxMemory / numberOfChunks;
        FileReaderService[] chunksFileReaders = new FileReaderService[numberOfChunks];
        initChunksReaders(chunksFileReaders);
        List<Queue<Record>> chunkQueues = new ArrayList<>(numberOfChunks);
        initChunksQueues(chunkQueues, numberOfChunks);
     //   ExecutorService singleThread = Executors.newSingleThreadExecutor();
        do {
            loadSortedRecords(chunksFileReaders, queueMaxSize, chunkQueues, executorService);
            Queue<Record> merged = merge(chunkQueues, queueMaxSize, comparator);
            mergeFinished = merged.isEmpty();
            recordsDBService.saveRecords(merged);
            //saveToDB(singleThread, recordsDBService, new LinkedList<>(merged));
            resultWriter.writeRecords(merged);
        } while (!mergeFinished);
        closeReaders(chunksFileReaders);
//        try {
//            singleThread.awaitTermination(30, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }


    private static void saveToDB(ExecutorService executorService,
                                 RecordsDBService recordsDBService, Queue<Record> records){
        executorService.execute(() -> recordsDBService.saveRecords(records));
    }

    private static void loadSortedRecords(FileReaderService[] fileServices, int queueMaxSize,
                                          List<Queue<Record>> chunksQueues, ExecutorService executorService) {
        CountDownLatch countDownLatch = new CountDownLatch(fileServices.length);
        for (int i = 0; i < fileServices.length; i++) {
            final int I = i;
            executorService.execute(() -> loadThread(I, fileServices, chunksQueues, queueMaxSize, countDownLatch));
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void loadThread(int i, FileReaderService[] fileServices, List<Queue<Record>> chunksQueues,
                                   int queueMaxSize, CountDownLatch countDownLatch) {
        try {
            FileReaderService chunkFileService = fileServices[i];
            Queue<Record> chunkRecords = chunksQueues.get(i);
            Record[] records = chunkFileService.readRecords(queueMaxSize - chunkRecords.size());
            chunkRecords.addAll(Arrays.asList(records));
            countDownLatch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Queue<Record> merge(List<Queue<Record>> chunksQueues, int queueMaxLength, Comparator<Record> comparator) {
        Queue<Record> merged = new LinkedList<>();

        while (merged.size() < queueMaxLength && anyQueueIsNotEmpty(chunksQueues)) {
            int minIndex = 0;
            while (chunksQueues.get(minIndex).isEmpty()) {
                minIndex++;
            }
            for (int i = minIndex + 1; i < chunksQueues.size(); i++) {
                if (!chunksQueues.get(i).isEmpty() &&
                        comparator.compare(chunksQueues.get(i).peek(), chunksQueues.get(minIndex).peek()) < 0) {

                    minIndex = i;
                }
            }
            merged.add(chunksQueues.get(minIndex).poll());
        }
        return merged;
    }

    private static boolean anyQueueIsNotEmpty(List<Queue<Record>> chunks) {
        for (Queue<Record> queue : chunks) {
            if (!queue.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static void closeReaders(FileReaderService[] readers) throws IOException {
        for (FileReaderService fileReader : readers) {
            fileReader.close();
        }
    }

    private static void initChunksQueues(List<Queue<Record>> chunkQueues, int numberOfChunks) {
        for (int i = 0; i < numberOfChunks; i++) {
            chunkQueues.add(new LinkedList<>());
        }
    }

    private static void initChunksReaders(FileReaderService[] chunksFileReaders) throws IOException, CsvValidationException {
        for (int i = 0; i < chunksFileReaders.length; i++) {
            chunksFileReaders[i] = new FileReaderServiceImpl(String.format(TEMP_FILE_NAME_FORMAT, i));
        }
    }
}
