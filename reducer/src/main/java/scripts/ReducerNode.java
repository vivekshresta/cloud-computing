package scripts;

import client.Client;
import client.HttpClient;
import functionality.api.Reducer;
import functionality.impl.InvertedIndexReducer;
import functionality.impl.WordCountReducer;
import helper.LogHelper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ReducerNode {
    private static final Logger oLog = LogHelper.getLogger(ReducerNode.class.getName());
    private static final String MASTER_IP_ADDRESS = "104.197.231.179";
    private static final String MASTER_PORT = "8080";

    public static void main(String[] args) {
        ReducerNode reducerNode = new ReducerNode();
        reducerNode.run();
    }

    private void run() {
        HttpClient httpClient = new HttpClient(MASTER_IP_ADDRESS, MASTER_PORT);
        Map<String, String> data = httpClient.getDataFromMaster("reducerdata");
        oLog.info(data.toString());
        Client client = null;
        try {
            client = new Client(data.get("kvStoreAddress"), Integer.parseInt(data.get("kvStorePort")));
            List<String[]> kvPairs = getKVPairs(client, "reducer_" + data.get("reducerID"));
            Reducer reducer = getReducer(data.get("functionalityName"));
            Map<String, String> reducedData = reduceData(reducer, kvPairs);
            writeToKVStore(client, reducedData, "output");
            httpClient.intimateMasterAboutCompletion("reducercomplete");
        } catch (Exception e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        } finally {
            if(client != null)
                client.destroy();
        }
    }

    private void writeToKVStore(Client client, Map<String, String> kvPairs, String fileID) {
        for(String key : kvPairs.keySet())
            client.appendToKVStore(fileID, key, kvPairs.get(key));
    }

    private Map<String, String> reduceData(Reducer reducer, List<String[]> kvPairs) {
        return reducer.reduce(kvPairs);
    }

    private List<String[]> getKVPairs(Client client, String fileID) {
        return client.getKVPairs(fileID);
    }

    private Reducer getReducer(String functionality) {
        return functionality.equals("WordCount") ? new WordCountReducer() : new InvertedIndexReducer();
    }
}
