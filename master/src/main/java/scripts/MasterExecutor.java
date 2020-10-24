package scripts;

import client.Client;
import client.google.ComputeEngine;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Operation;
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
    private final int numberOfMappers;
    private final int numberOfReducers;
    private final String functionalityName;
    private final String masterAddress;
    private final String masterPort;
    private final String kvStoreAddress;
    private final String kvStorePort;
    private Client kvStoreClient;

    public MasterExecutor(String filePath, String numberOfMappers, String numberOfReducers,
                          String functionalityName, String masterAddress, String masterPort, String kvStoreAddress, String kvStorePort) {
        this.filePath = filePath;
        this.numberOfMappers = Integer.parseInt(numberOfMappers);
        this.numberOfReducers = Integer.parseInt(numberOfReducers);
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

    public String getFinalOutput() {
        String output = kvStoreClient.getFileData("output.txt");
        return output.trim();
    }

    public void createMapperVMs() throws Exception {
        createVMs("mapper-", "mapper.sh");
    }

    public void createReducerVMs() throws Exception {
        createVMs("reducer-", "reducer.sh");
    }

    private void createVMs(String prefix, String script) throws Exception {
        Compute computeEngine = ComputeEngine.getComputeEngine();
        //Operation operation = ComputeEngine.startInstance(computeEngine, "master");
        List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < numberOfMappers; i++) {
            operations.add(ComputeEngine.startInstance(computeEngine, prefix + i, script));
        }

        //lets wait till all the execution is complete
        for (Operation operation : operations) {
            Operation.Error error = ComputeEngine.blockUntilComplete(computeEngine, operation, ComputeEngine.OPERATION_TIMEOUT_MILLIS);
            if (error == null)
                oLog.info("Success!");
            else
                oLog.warning(error.toPrettyString());
        }
    }

    public void waitTillProcessesExecution(Process[] processes) {
        int i = 0;
        while(i != processes.length)
            for (i = 0; i < processes.length; i++)
                if(processes[i].isAlive())
                    break;
    }

    public void createChunks() {
        try {
            double linesPerChunk = getNumberOfLinesPerChunk(filePath, numberOfReducers);
            File myObj = new File(filePath);
            Scanner myReader = new Scanner(myObj);

            for (int mapperId = 0; myReader.hasNextLine(); mapperId++)
                for(int i = 0; myReader.hasNextLine() && i < linesPerChunk; i++)
                    kvStoreClient.createDataChunk(myReader.nextLine().toLowerCase() + "\n", "mapper_" + mapperId);
        } catch (IOException e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        }
    }

    public double getNumberOfLinesPerChunk(String filePath, double numberOfReducers) throws IOException {
        String usingSystemProperty = System.getProperty("user.dir");
        Path path = Paths.get(usingSystemProperty + filePath);
        return Math.ceil(Files.lines(path).count() / numberOfReducers);
    }

    public void deleteVMs() throws Exception {
        Compute computeEngine = ComputeEngine.getComputeEngine();
        for (int i = 0; i < numberOfMappers; i++) {
            ComputeEngine.deleteInstance(computeEngine, "mapper_" + i);
        }

        for (int i = 0; i < numberOfReducers; i++) {
            ComputeEngine.deleteInstance(computeEngine, "reducer_" + i);
        }
    }

    public boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

}
