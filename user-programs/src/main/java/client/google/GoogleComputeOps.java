package client.google;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import helper.LogHelper;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

public class GoogleComputeOps {
    private static final Logger oLog = LogHelper.getLogger(GoogleComputeOps.class.getName());
    private static final String SOURCE_IMAGE_PREFIX =
            "https://www.googleapis.com/compute/v1/projects/";
    private static final String SOURCE_IMAGE_PATH =
            "vivekshresta-bandaru/global/images/cloud-image";

    private String instanceName;

    public GoogleComputeOps(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getIpAddressOfInstance() throws GeneralSecurityException, IOException {

        Compute compute = buildCompute();

        InstanceList instanceList = listInstances(compute);
        Optional<Instance> optionalInstance = instanceList.getItems().stream()
                .filter(instance -> instance.getName().equalsIgnoreCase(instanceName)).findAny();

        return optionalInstance.map(instance ->
                instance.getNetworkInterfaces().get(0).getAccessConfigs().get(0).getNatIP())
                .orElse(null);
    }

    public int startInstance(Metadata metadata, boolean isPreemptable) {
        try {
            Compute compute = buildCompute();
            // List out instances, looking for the one created by this sample app.
            boolean foundOurInstance = checkForInstance(compute);

            Operation op;
            if (foundOurInstance) {
                op = deleteInstance(compute, instanceName);
                waitForOperationCompletion(compute, op);
            }

            op = startInstance(compute, metadata, isPreemptable);
            waitForOperationCompletion(compute, op);
            return 0;
        } catch (Throwable e) {
            oLog.warning(e.getMessage());
            return -1;
        }
    }

    public int deleteInstance() {
        try {
            Compute compute = buildCompute();
            // List out instances, looking for the one created by this sample app.
            boolean foundOurInstance = checkForInstance(compute);

            Operation op;
            if (foundOurInstance) {
                op = deleteInstance(compute, instanceName);
                waitForOperationCompletion(compute, op);
            }
            return 0;
        } catch (Throwable e) {
            oLog.warning(e.getMessage());
            return -1;
        }
    }

    private Compute buildCompute() throws GeneralSecurityException, IOException {
        /* Global instance of the HTTP transport. */
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        // Authenticate using Google Application Default Credentials.
        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        // Create Compute Engine object for listing instances.
        return new Compute.Builder(httpTransport, Constants.JSON_FACTORY, credential)
                .setApplicationName(Constants.APPLICATION_NAME)
                .build();
    }

    private void waitForOperationCompletion(Compute compute, Operation op) throws Exception {

        // Call Compute Engine API operation and poll for operation completion status
        System.out.println("Waiting for operation completion...");
        Operation.Error error = blockUntilComplete(compute, op);
        if (error == null) {
            oLog.info("Success!");
        } else {
            oLog.warning(error.toPrettyString());
        }
    }

    // [START list_instances]
    private boolean checkForInstance(Compute compute) throws IOException {
        InstanceList instanceList = listInstances(compute);
        if (instanceList.getItems() == null || instanceList.getItems().size() < 1) {
            oLog.info("No instances found. Sign in to the Google Developers Console and create "
                    + "an instance at: https://console.developers.google.com/");
        }
        return  instanceList.getItems().stream().anyMatch((instance -> instance.getName().equals(instanceName)));
    }
    // [END list_instances]

    private InstanceList listInstances(Compute compute) throws IOException {
        Compute.Instances.List instances = compute.instances().list(Constants.PROJECT_ID, Constants.ZONE_NAME);
        return instances.execute();
    }

    // [START create_instances]
    public Operation startInstance(Compute compute, Metadata metadata, boolean isPreemptable) throws IOException {
        oLog.info("================== Starting New Instance ==================");

        // Create VM Instance object with the required properties.
        Instance instance = new Instance();

        instance.setName(instanceName);
        instance.setKind("compute#instance");
        instance.setCanIpForward(false);
        instance.setZone("projects/" + Constants.PROJECT_ID + "/zones/" + Constants.ZONE_NAME);
        instance.setMachineType("projects/" + Constants.PROJECT_ID + "/zones/" + Constants.ZONE_NAME + "/machineTypes/n1-standard-1");
        instance.setDeletionProtection(false);

        //Set Disks
        List<AttachedDisk> attachedDisks = new ArrayList<>();
        AttachedDisk attachedDisk = new AttachedDisk();
        AttachedDiskInitializeParams attachedDiskInitializeParams = new AttachedDiskInitializeParams();
        attachedDiskInitializeParams.setSourceImage(SOURCE_IMAGE_PREFIX + SOURCE_IMAGE_PATH);
        attachedDiskInitializeParams.setDiskType("projects/" + Constants.PROJECT_ID + "/zones/" + Constants.ZONE_NAME + "/diskTypes/pd-standard");
        attachedDiskInitializeParams.setDiskSizeGb((long) 10);
        attachedDisk.setInitializeParams(attachedDiskInitializeParams);
        attachedDisk.setAutoDelete(true);
        attachedDisk.setBoot(true);
        attachedDisk.setDeviceName(instanceName);
        attachedDisk.setKind("compute#attachedDisk");
        attachedDisk.setMode("READ_WRITE");
        attachedDisk.setType("PERSISTENT");
        attachedDisks.add(attachedDisk);
        instance.setDisks(attachedDisks);

        //Network Interfaces
        List<NetworkInterface> networkInterfaces = new ArrayList<>();
        NetworkInterface networkInterface = new NetworkInterface();
        networkInterface.setKind("compute#networkInterface");
        String region = Constants.ZONE_NAME.split("-")[0] + "-" + Constants.ZONE_NAME.split("-")[1];
        networkInterface.setSubnetwork("projects/" + Constants.PROJECT_ID + "/regions/" + region + "/subnetworks/default");
        List<AccessConfig> accessConfigs = new ArrayList<>();
        AccessConfig accessConfig = new AccessConfig();
        accessConfig.setKind("compute#accessConfig");
        accessConfig.setName("External NAT");
        accessConfig.setType("ONE_TO_ONE_NAT");
        accessConfig.setNetworkTier("PREMIUM");
        accessConfigs.add(accessConfig);
        networkInterface.setAccessConfigs(accessConfigs);
        networkInterfaces.add(networkInterface);
        instance.setNetworkInterfaces(networkInterfaces);

        //Scheduling
        Scheduling scheduling = new Scheduling();
        if (isPreemptable) {
            scheduling.setPreemptible(true);
            scheduling.setOnHostMaintenance("TERMINATE");
            scheduling.setAutomaticRestart(false);
        } else {
            scheduling.setPreemptible(false);
            scheduling.setOnHostMaintenance("MIGRATE");
            scheduling.setAutomaticRestart(true);
        }
        instance.setScheduling(scheduling);

        List<ServiceAccount> serviceAccounts = new ArrayList<>();
        ServiceAccount serviceAccount = new ServiceAccount();
        serviceAccount.setEmail("my-account@gopikiran-talangalashama.iam.gserviceaccount.com");
        List<String> scopes = new ArrayList<>();
        scopes.add("https://www.googleapis.com/auth/cloud-platform");
        serviceAccount.setScopes(scopes);
        serviceAccounts.add(serviceAccount);
        instance.setServiceAccounts(serviceAccounts);

        if (metadata != null) {
            instance.setMetadata(metadata);
        }

        oLog.info(instance.toPrettyString());
        Compute.Instances.Insert insert = compute.instances().insert(Constants.PROJECT_ID, Constants.ZONE_NAME, instance);
        return insert.execute();
    }
    // [END create_instances]

    private static Operation deleteInstance(Compute compute, String instanceName) throws Exception {
        oLog.info(
                "================== Deleting Instance " + instanceName + " ==================");
        Compute.Instances.Delete delete =
                compute.instances().delete(Constants.PROJECT_ID, Constants.ZONE_NAME, instanceName);
        return delete.execute();
    }

    // [START wait_until_complete]
    private Operation.Error blockUntilComplete(Compute compute, Operation operation) throws Exception {
        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000;
        String zone = operation.getZone();  // null for global/regional operations
        if (zone != null) {
            String[] bits = zone.split("/");
            zone = bits[bits.length - 1];
        }
        String status = operation.getStatus();
        String opId = operation.getName();
        while (operation != null && !status.equals("DONE")) {
            Thread.sleep(pollInterval);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= Constants.OPERATION_TIMEOUT_MILLIS) {
                throw new InterruptedException("Timed out waiting for operation to complete");
            }
            oLog.info("waiting...");
            if (zone != null) {
                Compute.ZoneOperations.Get get = compute.zoneOperations().get(Constants.PROJECT_ID, zone, opId);
                operation = get.execute();
            } else {
                Compute.GlobalOperations.Get get = compute.globalOperations().get(Constants.PROJECT_ID, opId);
                operation = get.execute();
            }
            if (operation != null) {
                status = operation.getStatus();
            }
        }
        return operation == null ? null : operation.getError();
    }
    // [END wait_until_complete]
}