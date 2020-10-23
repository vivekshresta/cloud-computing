package http.service;

import helper.LogHelper;
import helper.PostBodyParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scripts.MasterExecutor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@RestController
public class HttpAPI {
    private static final Logger oLog = LogHelper.getLogger(HttpAPI.class.getName());
    private int numberOfMappers;
    private int numberOfReducers;
    private String functionalityName;
    private String masterAddress;
    private String masterPort;
    private String kvStoreAddress;
    private String kvStorePort;

    @Autowired
    public PostBodyParser postBodyParser;

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

        Map<String, String> postBody = postBodyParser.getPostBodyInAMap(request);
        oLog.info("Data sent from client: " + postBody.toString());

        String fileLocation = postBody.get("fileLocation");
        try {
            numberOfMappers = Integer.parseInt(postBody.get("numberOfMappers"));
            numberOfReducers = Integer.parseInt(postBody.get("numberOfReducers"));
        } catch(Exception e) {
            oLog.warning("Failed parsing numbers");
        }

        functionalityName = postBody.get("functionalityName");
        masterAddress = postBody.get("masterAddress");
        masterPort = postBody.get("masterPort");
        kvStoreAddress = postBody.get("kvStoreAddress");
        kvStorePort = postBody.get("kvStorePort");

        MasterExecutor masterExecutor = new MasterExecutor(fileLocation, String.valueOf(numberOfMappers), String.valueOf(numberOfReducers),
                functionalityName, masterAddress, masterPort, kvStoreAddress, kvStorePort);
        String output = masterExecutor.run();
        oLog.info("Final output in output.txt in kv store");

        return ResponseEntity.ok(output);
    }

    @RequestMapping(value = "/destroy")
    public ResponseEntity<?> destroy(HttpServletRequest request, HttpServletResponse response) {
        System.out.println("yo");
        Map<String, String> result = new HashMap<>();
        //Map<String, String> postBody = postBodyParser.getPostBodyInAMap(request);
        result.put("Yo", "yo");

        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/mapperdata")
    public ResponseEntity<?> mapperData(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> data = new HashMap<>();
        data.put("mapperID", String.valueOf(--numberOfMappers));
        data.put("numberOfReducers", String.valueOf(numberOfReducers));
        data.put("functionalityName", functionalityName);
        data.put("kvStoreAddress", kvStoreAddress);
        data.put("kvStorePort", kvStorePort);

        return ResponseEntity.ok(data);
    }

    @RequestMapping(value = "/reducerdata")
    public ResponseEntity<?> reducerData(HttpServletRequest request, HttpServletResponse response) {
        Map<String, String> data = new HashMap<>();
        data.put("reducerID", String.valueOf(--numberOfReducers));
        data.put("functionalityName", functionalityName);
        data.put("kvStoreAddress", kvStoreAddress);
        data.put("kvStorePort", kvStorePort);

        return ResponseEntity.ok(data);
    }

}
