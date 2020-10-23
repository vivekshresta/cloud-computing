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
        String numberOfMappers = postBody.get("numberOfMappers");
        String numberOfReducers = postBody.get("numberOfReducers");
        String functionalityName = postBody.get("functionalityName");
        String masterAddress = postBody.get("masterAddress");
        String masterPort = postBody.get("masterPort");
        String kvStoreAddress = postBody.get("kvStoreAddress");
        String kvStorePort = postBody.get("kvStorePort");

        try {
            this.numberOfMappers = Integer.parseInt(numberOfMappers);
            this.numberOfReducers = Integer.parseInt(numberOfReducers);
        } catch(Exception e) {
            oLog.warning("Failed parsing numbers");
        }

        MasterExecutor masterExecutor = new MasterExecutor(fileLocation, numberOfMappers, numberOfReducers, functionalityName,
                masterAddress, masterPort, kvStoreAddress, kvStorePort);
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

    @RequestMapping(value = "/assignmapperid")
    public ResponseEntity<?> assignMapper(HttpServletRequest request, HttpServletResponse response) {
        return ResponseEntity.ok(--numberOfMappers);
    }

}
