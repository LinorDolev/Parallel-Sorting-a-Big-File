package sortBigFile;

import com.opencsv.exceptions.CsvValidationException;
import sortBigFile.fileIOServices.FileReaderService;
import sortBigFile.fileIOServices.FileReaderServiceImpl;
import sortBigFile.fileIOServices.FileWriterService;
import sortBigFile.fileIOServices.FileWriterServiceImpl;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Algorithm {
    private static final String TEMP_FILE_NAME_FORMAT = ".\\SORTED_CHUNK_%d.csv";

    /**
     * sortBigFile.Main function
     * @param filePath - path to big file to sort
     * @param resultPath - path to write the sorted array to.
     *
     *  Steps:
     *                   1. Read maxMemory sized chunks - once at a time - sort them and write them back to a file
     *                   2. Read maxMemory/numberOfChunks smallest records from each sorted chunk
     *                   3. Merge those records into one buffer (his size is maxMemory)
     *                   4. Write into a new file the maxMemory/numberOfChunks smallest records
     *                   5. Read the next maxMemory/(numberOfChunks)
     *                   6. Go back to 3 until there are no records left in any of the files.
     */
    public static void sortBigFile(String filePath, int fileSize, int maxMemory, String sortKey, String resultPath){
        int remainingChunk = fileSize % maxMemory == 0 ? 0 : 1;
        int numberOfChunks = (fileSize / maxMemory) + remainingChunk;

        try(FileReaderService fileReader = new FileReaderServiceImpl(filePath, true);
            FileWriterService resultWriter = new FileWriterServiceImpl(resultPath)){
            int sortKeyIndex = fileReader.indexOfKey(sortKey);
            resultWriter.writeRecord(fileReader.getKeys());
            Comparator<Record> comparator = Comparator.comparing(r -> r.get(sortKeyIndex));
            sortChunks(fileReader, numberOfChunks, maxMemory, comparator);
            mergeChunks(maxMemory, numberOfChunks, resultWriter, comparator);
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param fileReader - reader of the original big file that we want to sort
     * @param numberOfChunks - number of records in this file / max memory size
     * @param maxMemory - max number of records we are allowed to store in memory
     */
    private static void sortChunks(FileReaderService fileReader, int numberOfChunks, int maxMemory, Comparator<Record> comparator) throws IOException, CsvValidationException {
        for (int i = 0; i < numberOfChunks; i++) {
            Record[] records = fileReader.readRecords(maxMemory);
            Arrays.sort(records, comparator);
            String fileName = String.format(TEMP_FILE_NAME_FORMAT, i);
            File file = new File(fileName);
            file.deleteOnExit();
            FileWriterServiceImpl.writeRecordsToANewFile(file.getAbsolutePath(), records);
        }
    }

    private static void mergeChunks(int maxMemory, int numberOfChunks, FileWriterService resultWriter,
                             Comparator<Record> comparator) throws IOException, CsvValidationException {
        boolean mergeFinished;
        int queueMaxSize = maxMemory / numberOfChunks;
        FileReaderService[] chunksFileReaders = new FileReaderService[numberOfChunks];
        initChunksReaders(chunksFileReaders);
        List<Queue<Record>> chunkQueues = new ArrayList<>(numberOfChunks);
        initChunksQueues(chunkQueues, numberOfChunks);
        do{
            loadSortedRecords(chunksFileReaders, queueMaxSize, chunkQueues);
            Queue<Record> merged = merge(chunkQueues, queueMaxSize, comparator);
            mergeFinished = merged.isEmpty();
            resultWriter.writeRecords(merged);
        }while (!mergeFinished);
        closeReaders(chunksFileReaders);
    }

    private static void loadSortedRecords(FileReaderService[] fileServices, int queueMaxSize, List<Queue<Record>> chunksQueues) throws IOException, CsvValidationException {
        for (int i = 0; i < fileServices.length; i++) {
            FileReaderService chunkFileService = fileServices[i];
            Queue<Record> chunkRecords = chunksQueues.get(i);

            Record[] records = chunkFileService.readRecords(queueMaxSize - chunkRecords.size());
            chunkRecords.addAll(Arrays.asList(records));
        }
    }

    private static Queue<Record> merge(List<Queue<Record>> chunksQueues, int queueMaxLength, Comparator<Record> comparator){
        Queue<Record> merged = new LinkedList<>();
        while (merged.size() <  queueMaxLength && anyQueueIsNotEmpty(chunksQueues)) {
            int minIndex = 0;
            while(chunksQueues.get(minIndex).isEmpty()){
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
        for (Queue<Record> queue: chunks ) {
            if(!queue.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static void closeReaders(FileReaderService[] readers) throws IOException {
        for (FileReaderService fileReader: readers) {
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
