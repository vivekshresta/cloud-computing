package functionality.impl;

import functionality.api.Reducer;
import helper.LogHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class InvertedIndexReducer implements Reducer {
    private static final Logger oLog = LogHelper.getLogger(InvertedIndexReducer.class.getName());

    @Override
    public Map<String, String> reduce(List<String[]> kvPairs) {
        Map<String, String> result = new HashMap<>();
        Map<String, String> reducedDataAccordingToMapper = reduceSameMapperData(kvPairs);

        for(String key : reducedDataAccordingToMapper.keySet()) {
            String[] keys = key.split(" ");
            if(keys.length == 2) {
                String newValue = keys[1] + " " + reducedDataAccordingToMapper.get(key);
                result.compute(keys[0], (k, v) -> v == null ? newValue : v + " " + newValue);
            }
        }

        oLog.info("Reduced data: " + result.toString());

        return result;
    }

    private Map<String, String> reduceSameMapperData(List<String[]> kvPairs) {
        Map<String, String> reducedData = new HashMap<>();

        for(String[] kvPair : kvPairs) {
            String key = getKey(kvPair);
            reducedData.compute(key, (k,v) -> v == null ? "1" : String.valueOf(Integer.parseInt(v) + 1));
        }

        return reducedData;
    }

    protected String getKey(String[] kvPair) {
        return kvPair[0] + " " + kvPair[1];
    }
}
