package client.google;

import com.google.api.services.compute.model.Metadata;
import helper.LogHelper;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ComputeEngineHelper {
    private static final Logger LOGGER = LogHelper.getLogger(ComputeEngineHelper.class.getName());

    private static final String STARTUP_SCRIPT_URL_KEY = "startup-script-url";
    private static final String STARTUP_SCRIPT_URL_VALUE_MASTER = "gs://" + Constants.PROJECT_ID + "/init.sh";
    private static final String STARTUP_SCRIPT_URL_VALUE_KVSTORE = "gs://" + Constants.PROJECT_ID + "/kv_vm_startup.sh";
    private static final String script = "init.sh";

    public Map<String, String> initiateCluster(String kvStoreComponentName, String masterComponentName,
                                               int masterPort, int kvStorePort) throws IOException, GeneralSecurityException {

        Map<String, String> map = new HashMap<>();
        int status = -1;
        GoogleComputeOps gc = new GoogleComputeOps(kvStoreComponentName);

//        while (status != 0) {
//            status = gc.startInstance(getMetaDataForKVStore(kvStoreComponentName, kvStorePort), false);
//        }

//        map.put(kvStoreComponentName, gc.getIpAddressOfInstance());

        status = -1;
        gc = new GoogleComputeOps(masterComponentName);

        while (status != 0) {
            status = gc.startInstance(getMetaDataForMaster(masterComponentName, masterPort), false);
        }

        map.put(masterComponentName, gc.getIpAddressOfInstance());

        return map;
    }

    public List<String> runMapReduce(String kvStoreIpAddress, int kvStorePort, String masterIpAddress,
                                     int masterPort, int mapperCount, int reducerCount, String functionalityName) {
        return new ArrayList<>();
    }

    public boolean destroyCluster(String kvStoreComponentName, String masterComponentName) {

        LOGGER.info(String.format("Shutting down the instances, master: %s, kvstore: %s", masterComponentName, kvStoreComponentName));

        int status = -1;
        GoogleComputeOps gc = new GoogleComputeOps(masterComponentName);

        while (status != 0) {
            status = gc.deleteInstance();
        }

        status = -1;
        gc = new GoogleComputeOps(kvStoreComponentName);

        while (status != 0) {
            status = gc.deleteInstance();
        }

        return true;
    }

    private Metadata getMetaDataForKVStore(String componentName, int port) {

        Metadata metadata = new Metadata();
        List<Metadata.Items> itemsList = new ArrayList<>();
        itemsList.add(getItem(STARTUP_SCRIPT_URL_KEY, STARTUP_SCRIPT_URL_VALUE_KVSTORE));
        metadata.setItems(itemsList);
        return metadata;
    }

    private Metadata getMetaDataForMaster(String componentName, int port) {

        Metadata metadata = new Metadata();
        List<Metadata.Items> itemsList = new ArrayList<>();
        itemsList.add(getItem(STARTUP_SCRIPT_URL_KEY, STARTUP_SCRIPT_URL_VALUE_MASTER));
        metadata.setItems(itemsList);
        return metadata;
    }

    private Metadata.Items getItem(String key, String value) {
        Metadata.Items item = new Metadata.Items();
        item.setKey(key);
        item.setValue(value);

        return item;
    }
}
