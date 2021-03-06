package scripts;

import client.Client;
import client.HttpClient;
import functionality.api.Mapper;
import functionality.impl.InvertedIndexMapper;
import functionality.impl.WordCountMapper;
import helper.LogHelper;

import java.util.*;
import java.util.logging.Logger;

public class MapperNode {
    private static final Logger oLog = LogHelper.getLogger(MapperNode.class.getName());
    private static final String MASTER_IP_ADDRESS = "104.197.231.179";
    private static final String MASTER_PORT = "8080";

    private String fetchDataChunk(Client client, String fileID) {
        return client.getDataChunk(fileID);
    }

    private Mapper getMapper(String functionality) {
        return functionality.equals("WordCount") ? new WordCountMapper() : new InvertedIndexMapper();
    }

    private List<String[]> generateKVPairs(Mapper mapper, String dataChunk, String mapperID) {
        List<String[]> kvPairs = new ArrayList<>();

        StringTokenizer lineTokenizer = new StringTokenizer(dataChunk, "\n");
        while(lineTokenizer.hasMoreElements()) {
            String[] words = lineTokenizer.nextToken().split(" ");
            for(String word : words) {
                String[] kvPair = mapper.generateKeyValuePair(word, mapperID);
                if (kvPair != null)
                    kvPairs.add(kvPair);
            }
        }

        return kvPairs;
    }

    private void distributeKVPairs(Client client, List<String[]> kvPairs, int numberOfReducers) {
        for (String[] kvPair : kvPairs) {
            int assignedReducer = hash(kvPair[0], numberOfReducers);
            client.appendToKVStore("reducer_" + assignedReducer, kvPair);
        }
    }

    private int hash(String key, int n) {
        return Math.abs(key.hashCode() % n);
    }

    private void run() {
        HttpClient httpClient = new HttpClient(MASTER_IP_ADDRESS, MASTER_PORT);
        Map<String, String> data = httpClient.getDataFromMaster("mapperdata");
        oLog.info(data.toString());
        Client client = null;
        try {
            client = new Client(data.get("kvStoreAddress"), Integer.parseInt(data.get("kvStorePort")));
            String mapperID = data.get("mapperID");
            String dataChunk = fetchDataChunk(client, "mapper_" + mapperID);
            Mapper mapper = getMapper(data.get("functionalityName"));
            List<String[]> kvPairs = generateKVPairs(mapper, dataChunk, mapperID);
            distributeKVPairs(client, kvPairs, Integer.parseInt(data.get("numberOfReducers")));
            httpClient.intimateMasterAboutCompletion("mappercomplete");
        } catch (Exception e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        } finally {
            if(client != null)
                client.destroy();
        }
    }


    public static void main(String[] args) {
        //arg0 is mapperID
        //arg1 is numberOfReducers
        //arg2 is FunctionalityName
        //arg3 is masterAddress
        //arg4 is master port
        //arg5 is kv store address
        //arg6 is kv store port

        MapperNode mapperNode = new MapperNode();
        mapperNode.run();

    }

}
