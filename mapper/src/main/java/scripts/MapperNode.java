package scripts;


import client.Client;
import functionality.api.Mapper;
import functionality.impl.InvertedIndexMapper;
import functionality.impl.WordCountMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MapperNode {

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

    private void run(Client client, String functionality, int numberOfReducers, String mapperID) {
        String dataChunk = fetchDataChunk(client, "mapper_" + mapperID);
        Mapper mapper = getMapper(functionality);
        List<String[]> kvPairs = generateKVPairs(mapper, dataChunk, mapperID);
        distributeKVPairs(client, kvPairs, numberOfReducers);
    }

    public static void main(String[] args) {
        MapperNode mapperNode = new MapperNode();
        System.out.println(Arrays.toString(args));
        Client client = null;
        try {
            client = new Client(args[2], Integer.parseInt(args[3]));
            mapperNode.run(client, args[0], Integer.parseInt(args[1]), args[4]);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(client != null)
                client.destroy();
        }
    }

}
