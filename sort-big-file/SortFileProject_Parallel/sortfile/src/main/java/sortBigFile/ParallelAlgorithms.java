package sortBigFile;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelAlgorithms {

    private static double log2(int n){
        return Math.log(n) / Math.log(2);
    }

    private static int closestPowerOf2FromBelow(int n){
       double nlog2 = Math.floor(log2(n));
       return (int)Math.pow(2, nlog2);
    }

    public static <T> void parallelMergeSort(T[] array, Comparator<T> comparator) throws InterruptedException{
        final int NUM_THREADS = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        parallelMergeSort(array, comparator, executorService);
    }

    public static <T> void parallelMergeSort(T[] array, Comparator<T> comparator, ExecutorService executorService) throws InterruptedException {
        for (int i = 1; i <= (array.length / 2) + 1; i *= 2) {
            int step = i * 2;
            CountDownLatch countDownLatch = new CountDownLatch(array.length/step);
            for (int j = i; j < array.length; j += step) {
                final int I = i, J = j;
                executorService.submit(() -> mergeThread(array, I, J, comparator, countDownLatch));
            }
            countDownLatch.await();
        }
        merge(array, 0,  closestPowerOf2FromBelow(array.length), array.length, comparator);
    }

    private static <T> void mergeThread(T[] array, int i, int j, Comparator<T> comparator,
                                        CountDownLatch countDownLatch){
        merge(array, j - i, j, Math.min(j + i, array.length), comparator);
        countDownLatch.countDown();
    }

    private static <T> void  merge(T[] array, int l, int m, int r, Comparator<T> comparator){
        int i = 0, j = 0, k = l;
        T[] left = Arrays.copyOfRange(array, l, m);
        T[] right = Arrays.copyOfRange(array, m, r);

        while(i < left.length && j < right.length && k < r) {
            if(comparator.compare(left[i], right[j]) < 0) {
                array[k++] = left[i++];
            }
            else {
                array[k++] = right[j++];
            }
        }

        /* Collect remaining elements */
        while(i < left.length) {
            array[k++] = left[i++];
        }
        while(j < right.length) {
            array[k++] = right[j++];
        }

    }
}
