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

    private static final Logger logger = Logger.getLogger(Census.class.getName()); // TODO: create log

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
        // get the age and corresponding count that from a certain region
        Map<Integer, Integer> ageCountMap = getAgeCount(region);
        // get the top 3 age and count
        List<Map.Entry<Integer, Integer>> top3AgesList = getTop3AgesList(ageCountMap);
        // format
        return formatTop3Ages(top3AgesList);
    }

    public String[] formatTop3Ages(List<Map.Entry<Integer, Integer>> top3AgesList) {
        String[] top3Ages = new String[top3AgesList.size()]; // TODO: initialize the String[]
        int position = 1;
        int currentCount = top3AgesList.isEmpty() ? 0 : top3AgesList.get(0).getValue();
        for (int i = 0; i < top3AgesList.size(); i++) { // TODO: for loop using int i
            int count = top3AgesList.get(i).getValue();
            int age = top3AgesList.get(i).getKey();
            if (currentCount != count) {
                position++;
                currentCount = count;
            }
            top3Ages[i] = String.format(OUTPUT_FORMAT, position, age, count);
        }
        return top3Ages;
    }

    public Map<Integer, Integer> getAgeCount(String region) {
        Map<Integer, Integer> ageCountMap = new HashMap<>();
        try (AgeInputIterator iterator = iteratorFactory.apply(region)) { // TODO: try-with-resource
            while (iterator.hasNext()) {
                int age = iterator.next();
                if (age < 0) {
                    break;
                }
                ageCountMap.merge(age, 1, Integer::sum); // TODO: merge 1
            }
        } catch (Exception e) {
            logger.severe("Failed to process region: " + region);
        }
        return ageCountMap;
    }

    public List<Map.Entry<Integer, Integer>> getTop3AgesList(Map<Integer, Integer> ageCountMap) {
        // sort the Map via count desc, age asc
        List<Map.Entry<Integer, Integer>> sortedList = new ArrayList<>(ageCountMap.entrySet()); // TODO: can't pass hashMap to list
        sortedList.sort(Map.Entry.<Integer, Integer>comparingByValue(Comparator.reverseOrder())
                .thenComparing(Map.Entry.comparingByKey()));

        List<Map.Entry<Integer, Integer>> top3List = new ArrayList<>();
        // select the top 3
        int position = 1; // TODO: only care about the position, instead of entry.getKey()
        int currentCount = sortedList.isEmpty() ? 0 : sortedList.get(0).getValue();
        for (Map.Entry<Integer, Integer> entry : sortedList) { // TODO: iterator the entry-set list
            if (position == 3) {
                break; // TODO: kill the for loop
            }
            if (entry.getValue() != currentCount) {
                position++;
                currentCount = entry.getValue();
            }
            top3List.add(entry);
        }
        return top3List;
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        // get the age and corresponding count that from a certain region
        Map<Integer, Integer> ageCountMap = getAgeCount(regionNames);
        // get the top 3 age and count
        List<Map.Entry<Integer, Integer>> top3AgesList = getTop3AgesList(ageCountMap);
        // format
        return formatTop3Ages(top3AgesList);
    }

    public Map<Integer, Integer> getAgeCount(List<String> regionNames) {
        ExecutorService executor = Executors.newFixedThreadPool(CORES); // TODO: create executor
        Map<Integer, Integer> ageCountMap = new HashMap<>();
        try {
            List<Callable<Map<Integer, Integer>>> tasks = new ArrayList<>();
            for (String region : regionNames) {
                tasks.add(() -> getAgeCount(region)); // TODO: lambda
            }
            List<Future<Map<Integer, Integer>>> futures = executor.invokeAll(tasks);
            for (Future<Map<Integer, Integer>> f : futures) {
                for (Map.Entry<Integer, Integer> entry : f.get().entrySet()) { // TODO: iterator entry set to map.entry
                    int age = entry.getKey();
                    int count = entry.getValue();
                    ageCountMap.merge(age, count, Integer::sum);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.severe("Failed to process regions: " + regionNames + e);
        } finally {
            executor.shutdown();
        }
        return ageCountMap;
    }

    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}