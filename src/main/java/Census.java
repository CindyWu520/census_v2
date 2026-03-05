import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    private final Logger logger = Logger.getLogger(Census.class.getName());

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        Map<Integer, Integer> ageCountMap = getCountAges(region);
        List<Map.Entry<Integer, Integer>> top3List = findTop3Ages(ageCountMap);
        return formatTop3ToString(top3List);
    }

    private String[] formatTop3ToString(List<Map.Entry<Integer, Integer>> top3List) {
        String[] result = new String[top3List.size()];

        // start from 1st position
        int position = 1;
        int concurrent = top3List.isEmpty() ? 0 : top3List.get(0).getValue();
        for (int i = 0; i < top3List.size(); i++) {
            Map.Entry<Integer, Integer> entry = top3List.get(i);
            if (entry.getValue() != concurrent) {
                // lower count then change to lower position
                position++;
                concurrent = entry.getValue();
            }
            result[i] = String.format(OUTPUT_FORMAT, position, entry.getKey(), entry.getValue());
        }
        return result;
    }

    private List<Map.Entry<Integer, Integer>> findTop3Ages(Map<Integer, Integer> ageCountMap) {
        // sort by count DESC, then tie-break by age
        List<Map.Entry<Integer, Integer>> sortedList = new ArrayList<>(ageCountMap.entrySet());
        sortedList.sort(Map.Entry.<Integer, Integer>comparingByValue(Comparator.reverseOrder())
                .thenComparing(Map.Entry.comparingByKey()));

        List<Map.Entry<Integer, Integer>> top3List = new ArrayList<>();
        // rank position: 0:1st, 1:2nd, 2:3rd
        int position = 0;
        // start from the highest count
        int currentCount = sortedList.isEmpty() ? 0 : sortedList.get(0).getValue();
        for (Map.Entry<Integer, Integer> entry : sortedList) {
            if (position < 2 & entry.getValue().equals(currentCount)) {
                // same count, same position
                top3List.add(entry);
            } else if (position < 2) {
                // lower count, lower position
                top3List.add(entry);
                position++;
                currentCount = entry.getValue();
            }
        }
        return top3List;
    }

    /**
     * calculate the age and count for a single region
     *
     * @param region a single region
     * @return return a map of age(key) and count(value)
     */
    private Map<Integer, Integer> getCountAges(String region) {
        Map<Integer, Integer> countAge = new HashMap<>();
        try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
            while (iterator.hasNext()) {
                Integer age = iterator.next();
                if (countAge.containsKey(age)) {
                    countAge.put(age, countAge.get(age) + 1);
                } else {
                    countAge.put(age, 1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to iterate region: " + region, e);
        }
        return countAge;
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        try {
            Map<Integer, Integer> ageCountMap = getCountAgesAsync(regionNames);
            List<Map.Entry<Integer, Integer>> top3AgesList = findTop3Ages(ageCountMap);
            return formatTop3ToString(top3AgesList);
        } catch (InterruptedException | ExecutionException e) {
            logger.severe("failed to process region: "+ regionNames + e);
            return new String[]{};
        }
    }

    /**
     * calculate the age and count for a list region
     *
     * @param regionNames a list of region
     * @return return a map of age(key) and count(value)
     */
    private Map<Integer, Integer> getCountAgesAsync(List<String> regionNames) throws InterruptedException, ExecutionException{
        ExecutorService executor = Executors.newFixedThreadPool(CORES);
        try {
            List<Callable<Map<Integer, Integer>>> tasks = new ArrayList<>();
            for (String region : regionNames) {
                tasks.add(() -> getCountAges(region));
            }
            List<Future<Map<Integer, Integer>>> futures = executor.invokeAll(tasks);

            // merge all regions into one map
            Map<Integer, Integer> result = new HashMap<>();
            for (Future<Map<Integer, Integer>> future : futures) {
                Map<Integer, Integer> regionMap = future.get();
                for (Map.Entry<Integer, Integer> entry : regionMap.entrySet()) {
                    int age = entry.getKey();
                    int count = entry.getValue();
                    if (result.containsKey(age)) {
                        // add count across regions
                        result.put(age, result.get(age) + count);
                    } else {
                        result.put(age, count);
                    }
                }
            }
            return result;
        } finally {
            executor.shutdown();
        }
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}