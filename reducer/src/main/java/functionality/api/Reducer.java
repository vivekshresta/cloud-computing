package functionality.api;

import java.util.List;
import java.util.Map;

public interface Reducer {
    Map<String, String> reduce(List<String[]> kvPairs);
}
