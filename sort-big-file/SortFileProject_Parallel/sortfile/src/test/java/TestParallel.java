import org.junit.Test;
import sortBigFile.ParallelAlgorithms;

import java.util.Arrays;

public class TestParallel {
    @Test
    public void testParallelSort() throws InterruptedException {
        final int SIZE = 100;
        long t0;
        Integer[] numbers = new Integer[SIZE];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = (int) (Math.random() * SIZE * SIZE);
        }
        Integer[] expected = Arrays.copyOf(numbers, numbers.length);
        t0 = System.currentTimeMillis();
        ParallelAlgorithms.parallelMergeSort(numbers, Integer::compare);
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - t0)/ 1000.0);
        System.out.println(Arrays.toString(numbers));
        Arrays.sort(expected);
        System.out.println("\n=================================================\n");
        System.out.println(Arrays.toString(expected));
        assert Arrays.equals(numbers, expected);
    }
}
