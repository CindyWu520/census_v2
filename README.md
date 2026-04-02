# Census

A thread-safe, stateless Java class that calculates the top 3 most common ages across one or more regions.

## Requirements

- Java 8+
- No external dependencies

## Setup
### iterator_wrapper_architecture
![iterator_wrapper_architecture](docs/iterator_wrapper_architecture.svg)
```java
// 1. Implement AgeInputIterator to provide your data source
public class RegionAgeIterator implements Census.AgeInputIterator {
    private final Iterator<Integer> it;

    public RegionAgeIterator(String region) {
        // load ages from your data source e.g. DB, file, API
        this.it = loadAgesFromDatabase(region).iterator();
    }

    @Override
    public boolean hasNext() { return it.hasNext(); }

    @Override
    public Integer next() { return it.next(); }

    @Override
    public void close() {
        // close DB connection, file handle etc.
    }
}

// 2. Create a Census instance with your iterator factory
Census census = new Census(region -> new RegionAgeIterator(region));
```

## Usage

### Single Region

```java
String[] result = census.top3Ages("North");
```

### Multiple Regions (Parallel)

```java
List<String> regions = List.of("North", "South", "East");
String[] result = census.top3Ages(regions);
```

### Print Results

```java
for (String s : result) {
    System.out.println(s);
}
// Output:
// 1:10=38
// 2:15=35
// 3:12=30
```

## Output Format

Results are returned as `String[]` in the format:

```
Position:Age=Total
```

| Field | Description |
|---|---|
| `Position` | Rank (1st, 2nd, 3rd) |
| `Age` | The age value |
| `Total` | How many times this age appeared |

## Methods

### `top3Ages(String region)`
Returns the top 3 most common ages for a single region.

| Parameter | Type | Description |
|---|---|---|
| `region` | `String` | The name of the region to process |

**Returns:** `String[]` — top 3 ages in `Position:Age=Total` format

---

### `top3Ages(List<String> regionNames)`
Returns the top 3 most common ages across all regions combined using parallel processing.

| Parameter | Type | Description |
|---|---|---|
| `regionNames` | `List<String>` | List of region names to process |

**Returns:** `String[]` — top 3 ages in `Position:Age=Total` format  
**Returns:** `String[]{}` empty array if processing fails

## Design Decisions

### Stateless & Thread Safe
The class holds no mutable state — `iteratorFactory` is final and read-only. Each method call creates its own local data structures, making it safe to call from multiple threads simultaneously.

### Parallel Processing
`top3Ages(List<String>)` creates a fixed thread pool sized to the number of available CPU cores:
```java
private static final int CORES = Runtime.getRuntime().availableProcessors();
ExecutorService executor = Executors.newFixedThreadPool(CORES);
```
Each region is processed concurrently in its own thread. Results are merged sequentially on the main thread afterwards. The executor is always shut down in a `finally` block to prevent resource leaks.

### Sorting
Ages are sorted by count descending. Ties are broken by age ascending (smaller age ranks higher):
```
Age 10 → count 38 → position 1
Age 15 → count 35 → position 2
Age 12 → count 30 → position 3
```

### Tie Handling
When multiple ages share the same count, they share the same position rank:
```
Age 10 → count 38 → position 1
Age 15 → count 38 → position 1  ← tie, same rank
Age 12 → count 30 → position 2  ← not position 3
```

### Invalid Data
Negative ages are skipped — they are not valid census data:
```java
if (age < 0) {
    continue;
}
```

### Resource Management
`AgeInputIterator` is always closed after use via `try-with-resources`, ensuring no resource leaks even if an exception occurs mid-iteration:
```java
try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
    // iterate safely
}
```

## Error Handling

| Scenario | Behaviour |
|---|---|
| Iterator fails for a region | Throws `RuntimeException` with region name and cause |
| Thread interrupted during parallel processing | Logs error, returns empty `String[]` |
| Thread execution fails during parallel processing | Logs error, returns empty `String[]` |

## Interface

Provide your own implementation of `AgeInputIterator` to connect Census to your data source:

```java
public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    // Iterator<Integer> — return ages via next()
    // Closeable        — release resources via close()
}
```

## Internal Methods

| Method | Description |
|---|---|
| `getCountAges(String region)` | Iterates a single region, returns `Map<age, count>` |
| `getCountAgesAsync(List<String>)` | Processes multiple regions in parallel, returns merged `Map<age, count>` |
| `findTop3Ages(Map)` | Sorts and selects top 3 entries from the frequency map |
| `formatTop3ToString(List)` | Formats top 3 entries into `Position:Age=Total` strings |
