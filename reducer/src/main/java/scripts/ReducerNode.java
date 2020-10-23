package scripts;

import client.Client;
import functionality.api.Reducer;
import functionality.impl.InvertedIndexReducer;
import functionality.impl.WordCountReducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReducerNode {

    public static void main(String[] args) {
        ReducerNode reducerNode = new ReducerNode();
        System.out.println(Arrays.toString(args));
        Client client = null;
        try {
            client = new Client(args[1], Integer.parseInt(args[2]));
            reducerNode.run(client, args[0], args[3]);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(client != null)
                client.destroy();
        }
    }

    private void run(Client client, String functionality, String reducerID) {
        List<String[]> kvPairs = getKVPairs(client, "reducer_" + reducerID);
        Reducer reducer = getReducer(functionality);
        Map<String, String> reducedData = reduceData(reducer, kvPairs);
        writeToKVStore(client, reducedData, "output");
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
