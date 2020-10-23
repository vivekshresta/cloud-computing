package scripts;

import client.Client;
import helper.LogHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

public class MasterExecutor {
    private static final Logger oLog = LogHelper.getLogger(MasterExecutor.class.getName());
    private final String filePath;
    private final String numberOfMappers;
    private final String numberOfReducers;
    private final String functionalityName;
    private final String masterAddress;
    private final String masterPort;
    private final String kvStoreAddress;
    private final String kvStorePort;
    private Client kvStoreClient;

    public MasterExecutor(String filePath, String numberOfMappers, String numberOfReducers,
                          String functionalityName, String masterAddress, String masterPort, String kvStoreAddress, String kvStorePort) {
        this.filePath = filePath;
        this.numberOfMappers = numberOfMappers;
        this.numberOfReducers = numberOfReducers;
        this.functionalityName = functionalityName;
        this.masterAddress = masterAddress;
        this.masterPort = masterPort;
        this.kvStoreAddress = kvStoreAddress;
        this.kvStorePort = kvStorePort;

        try {
            this.kvStoreClient = new Client(kvStoreAddress, Integer.parseInt(kvStorePort));
        } catch (IOException e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        }
    }

    public String run() {
        createChunks();
        oLog.info("Chunks created");
        //Process[] mappers = createProcesses(true, Integer.parseInt(numberOfMappers), MapperNode.class.getName());
        //waitTillProcessesExecution(mappers);
        oLog.info("Mappers execution completed");
        //Process[] reducers = createProcesses(Integer.parseInt(numberOfMappers), ReducerNode.class.getName());
        //waitTillProcessesExecution(reducers);
        oLog.info("Reducers execution completed");
        return getFinalOutput();
    }

    private String getFinalOutput() {
        String output = kvStoreClient.getFileData("output.txt");
        return output.trim();
    }

    private void waitTillProcessesExecution(Process[] processes) {
        int i = 0;
        while(i != processes.length)
            for (i = 0; i < processes.length; i++)
                if(processes[i].isAlive())
                    break;
    }

    private Process[] createProcesses(int numberOfProcesses, String className) {
        return createProcesses(false, numberOfProcesses, className);
    }

    private Process[] createProcesses(boolean shouldAddNumberOfReducers, int numberOfProcesses, String className) {
        Process[] processes = new Process[numberOfProcesses];

        List<String> command = constructArguments(shouldAddNumberOfReducers, className);
        oLog.info("Arguments while constructing a process: " + command);
        ProcessBuilder builder = new ProcessBuilder(command);
        try {
            for (int i = 0; i < numberOfProcesses; i++) {
                command.add(String.valueOf(i));
                processes[i] = builder.inheritIO().start();
                command.remove(command.size() - 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return processes;
    }

    private List<String> constructArguments(boolean shouldAddNumberOfReducers, String className) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classPath = System.getProperty("java.class.path");

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classPath);
        command.add(className);
        command.addAll(getProcessStartArguments(shouldAddNumberOfReducers));

        return command;
    }

    private List<String> getProcessStartArguments(boolean shouldAddNumberOfReducers) {
        List<String> args = new ArrayList<>();
        args.add(functionalityName);
        if(shouldAddNumberOfReducers)
            args.add(numberOfReducers);

        args.add(kvStoreAddress);
        args.add(String.valueOf(kvStorePort));

        return args;
    }

    private void createChunks() {
        try {
            double linesPerChunk = getNumberOfLinesPerChunk(filePath, Integer.parseInt(numberOfReducers));
            File myObj = new File(filePath);
            Scanner myReader = new Scanner(myObj);

            for (int mapperId = 0; myReader.hasNextLine(); mapperId++)
                for(int i = 0; myReader.hasNextLine() && i < linesPerChunk; i++)
                    kvStoreClient.createDataChunk(myReader.nextLine().toLowerCase() + "\n", "mapper_" + mapperId);
        } catch (IOException e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        }
    }

    private double getNumberOfLinesPerChunk(String filePath, double numberOfReducers) throws IOException {
        Path path = Paths.get(filePath);
        return Math.ceil(Files.lines(path).count() / numberOfReducers);
    }

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

}
