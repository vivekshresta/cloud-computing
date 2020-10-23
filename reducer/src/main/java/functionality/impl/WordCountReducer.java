package functionality.impl;

import functionality.api.Reducer;
import helper.LogHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class WordCountReducer implements Reducer {
    private static final Logger oLog = LogHelper.getLogger(WordCountReducer.class.getName());

    @Override
    public Map<String, String> reduce(List<String[]> kvPairs) {
        Map<String, String> reducedData = new HashMap<>();

        for(String[] kvPair : kvPairs)
            reducedData.compute(kvPair[0], (k,v) -> v == null ? "1" : String.valueOf(Integer.parseInt(v) + 1));

        oLog.info("Reduced data: " + reducedData);
        return reducedData;
    }
}
