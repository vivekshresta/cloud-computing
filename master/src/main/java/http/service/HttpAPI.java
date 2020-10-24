package http.service;

import helper.LogHelper;
import helper.PostBodyParser;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scripts.MasterExecutor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@RestController
public class HttpAPI {
    private static final Logger oLog = LogHelper.getLogger(HttpAPI.class.getName());
    private static final String MASTER_IP_ADDRESS = "104.197.231.179";
    private static final String MASTER_PORT = "8080";
    private static final String KV_STORE_IP_ADDRESS = "34.123.248.115";
    private static final String KV_STORE_PORT = "10254";

    private MasterExecutor masterExecutor;
    private int numberOfMappers;
    private int numberOfReducers;
    private String functionalityName;
    private String masterAddress;
    private String masterPort;
    private String kvStoreAddress;
    private String kvStorePort;
    private int mappersCount;
    private int reducersCount;
    private String output;

    @RequestMapping(value = "/abc")
    public ResponseEntity<?> test(HttpServletRequest request, HttpServletResponse response) {
        System.out.println("yo");
        Map<String, String> result = new HashMap<>();
        //Map<String, String> postBody = postBodyParser.getPostBodyInAMap(request);
        result.put("Yo", "yo");

        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/mapreduce")
    public ResponseEntity<?> mapReduce(HttpServletRequest request, HttpServletResponse response) {
        //arg0 is input file location
        //arg1 is numberOfMappers
        //arg2 is numberOfReducers
        //arg3 is FunctionalityName
        //arg4 is masterAddress
        //arg5 is master port
        //arg6 is kv store address
        //arg7 is kv store port

        Map<String, String> result = new HashMap<>();
        PostBodyParser postBodyParser = new PostBodyParser();
        Map<String, String> postBody = postBodyParser.getParams(request);
        oLog.info("Data sent from client: " + postBody.toString());

        String fileLocation = postBody.get("fileLocation");
        try {
            numberOfMappers = Integer.parseInt(postBody.get("numberOfMappers"));
            numberOfReducers = Integer.parseInt(postBody.get("numberOfReducers"));
            mappersCount = numberOfMappers;
            reducersCount = numberOfReducers;
        } catch(Exception e) {
            oLog.warning("Failed parsing numbers");
        }

        functionalityName = postBody.get("functionalityName");
        masterAddress = postBody.get("masterAddress");
        masterPort = postBody.get("masterPort");
        kvStoreAddress = postBody.get("kvStoreAddress");
        kvStorePort = postBody.get("kvStorePort");

        masterExecutor = new MasterExecutor(fileLocation, String.valueOf(numberOfMappers), String.valueOf(numberOfReducers),
                functionalityName, masterAddress, masterPort, kvStoreAddress, kvStorePort);
        masterExecutor.createChunks();
        oLog.info("Chunks created");
        try {
            masterExecutor.createMapperVMs();
            oLog.info("Mappers execution completed");
            result.put("status", "created mappers");
        } catch (Exception e) {
            oLog.warning("Failed creating masters");
            oLog.warning(e.getMessage());
            result.put("status", "failed creating masters");
        }

        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/destroy")
    public ResponseEntity<?> destroy(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> result = new HashMap<>();
        try {
            masterExecutor.deleteVMs();
            oLog.info("Successfully deleted the VMs");
            result.put("status", "success");
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            oLog.warning(Arrays.toString(e.getStackTrace()));
            result.put("status", "fail");
        }

        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/mapperdata")
    public ResponseEntity<?> mapperData(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> data = new HashMap<>();
        data.put("mapperID", String.valueOf(--numberOfMappers));
        data.put("numberOfReducers", String.valueOf(numberOfReducers));
        data.put("functionalityName", functionalityName);
        data.put("kvStoreAddress", KV_STORE_IP_ADDRESS);
        data.put("kvStorePort", KV_STORE_PORT);

        return ResponseEntity.ok(data);
    }

    @RequestMapping(value = "/reducerdata")
    public ResponseEntity<?> reducerData(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> data = new HashMap<>();
        data.put("reducerID", String.valueOf(--numberOfReducers));
        data.put("functionalityName", functionalityName);
        data.put("kvStoreAddress", KV_STORE_IP_ADDRESS);
        data.put("kvStorePort", KV_STORE_PORT);

        return ResponseEntity.ok(data);
    }

    @RequestMapping(value = "/mappercomplete")
    public ResponseEntity<?> mapperExecutionComplete(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> result = new HashMap<>();
        numberOfMappers++;
        System.out.println(numberOfMappers);
        if(numberOfMappers == mappersCount) {
            //spawn reducer VM's
            try {
                masterExecutor.createReducerVMs();
                result.put("status", "spawned");
                return ResponseEntity.ok(result);
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        result.put("status", "waiting");
        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/reducercomplete")
    public ResponseEntity<?> reducerExecutionComplete(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> result = new HashMap<>();
        numberOfReducers++;
        if(numberOfReducers == reducersCount) {
            //execution complete
            try {
                output = masterExecutor.getFinalOutput();
                oLog.info(output);
                result.put("status", "completed");
                return ResponseEntity.ok(result);
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        result.put("status", "waiting");
        return ResponseEntity.ok(result);
    }

}
