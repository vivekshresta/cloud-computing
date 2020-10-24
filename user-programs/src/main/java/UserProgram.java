
import client.google.ComputeEngine;
import client.google.ComputeEngineHelper;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Operation;
import helper.LogHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class UserProgram {
    private static Logger oLog = LogHelper.getLogger(UserProgram.class.getName());
    private static final String MASTER_IP_ADDRESS = "104.197.231.179";
    private static final String MASTER_PORT = "8080";
    private static final String KV_STORE_IP_ADDRESS = "34.123.248.115";
    private static final String KV_STORE_PORT = "10254";

    public static void main(String[] args) {
        //arg0 is input file location
        //arg1 is numberOfMappers
        //arg2 is numberOfReducers
        //arg3 is FunctionalityName
        //arg4 is ipaddress
        //arg5 is port number

        oLog.info("Starting User program");
//        try {
////            Compute computeEngine = ComputeEngine.getComputeEngine();
////            Map<String, String> vms = new ComputeEngineHelper().initiateCluster("KVStore",
////                    "test-vm", 8080, 8080);
//            Compute computeEngine = ComputeEngine.getComputeEngine();
//            //Operation operation = ComputeEngine.startInstance(computeEngine, "master");
//            Operation operation = ComputeEngine.startInstance(computeEngine, "kv-store");
//            System.out.println("Waiting for operation completion...");
//            Operation.Error error = ComputeEngine.blockUntilComplete(computeEngine, operation, ComputeEngine.OPERATION_TIMEOUT_MILLIS);
//            if (error == null) {
//                System.out.println("Success!");
//            } else {
//                System.out.println(error.toPrettyString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Process kvStore = createProcess(new ArrayList<>(), Server.class.getName());
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        oLog.info("Created KV store");
//
//        List<String> processStartArguments = getProcessStartArguments(args[0], args[1], args[2], args[3], args[4], args[5]);
//        Process mapReduce = createProcess(processStartArguments, MasterNode.class.getName());
//        oLog.info("Created Master VM");
//
//        //fault tolerance
//        boolean shouldExitKV = false, shouldExitMR = false;
//        while(!shouldExitKV && !shouldExitMR) {
//            if(kvStore.isAlive()) {
//                shouldExitKV = false;
//            } else {
//                if(kvStore.exitValue() == 0) {
//                    oLog.info("KV store exited successfully");
//                    shouldExitKV = true;
//                } else {
//                    oLog.warning("Error in KV Store. Restarting it");
//                    kvStore = createProcess(new ArrayList<>(), Server.class.getName());
//                }
//            }
//
//            if(mapReduce.isAlive()) {
//                shouldExitMR = false;
//            } else {
//                if(mapReduce.exitValue() == 0) {
//                    oLog.info("Master node completed execution successfully");
//                    shouldExitMR = true;
//                } else {
//                    oLog.warning("Error in Master node. Restarting it");
//                    mapReduce = createProcess(processStartArguments, Server.class.getName());
//                }
//            }
//        }

    }

    private static Process createProcess(List<String> processStartArgs, String className) {
        Process process = null;
        List<String> command = constructArguments(processStartArgs, className);
        ProcessBuilder builder = new ProcessBuilder(command);
        try {
            process = builder.inheritIO().start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return process;
    }

    private static List<String> constructArguments(List<String> processStartArgs, String className) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classPath = System.getProperty("java.class.path");

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classPath);
        command.add(className);
        command.addAll(processStartArgs);

        return command;
    }

    private static List<String> getProcessStartArguments(String fileLocation, String numberOfMappers, String numberOfReducers, String functionality, String inetAddress, String port) {
        List<String> args = new ArrayList<>();
        args.add(fileLocation);
        args.add(numberOfMappers);
        args.add(numberOfReducers);
        args.add(functionality);
        args.add(inetAddress);
        args.add(port);

        return args;
    }
}
