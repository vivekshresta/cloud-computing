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
    public static final String MASTER_IPADDRESS = "34.123.248.115";
    public static final String MASTER_PORT = "8080";

    public static void main(String[] args) {
        ReducerNode reducerNode = new ReducerNode();
        reducerNode.run();
    }

    private void run() {
        Map<String, String> postBody = new HttpClient(MASTER_IPADDRESS, MASTER_PORT, "reducerdata").getDataFromMaster();
        oLog.info(postBody.toString());
        Client client = null;
        try {
            client = new Client(postBody.get("kvStoreAddress"), Integer.parseInt(postBody.get("kvStorePort")));
            List<String[]> kvPairs = getKVPairs(client, "reducer_" + postBody.get("reducerID"));
            Reducer reducer = getReducer(postBody.get("functionalityName"));
            Map<String, String> reducedData = reduceData(reducer, kvPairs);
            writeToKVStore(client, reducedData, "output");
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
