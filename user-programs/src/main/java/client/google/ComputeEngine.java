package client.google;

/*
 * Copyright 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.Lists;
import helper.LogHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Command-line sample to demo listing Google Compute Engine instances using Java and the Google
 * Compute Engine API.
 */
public class ComputeEngine {
    private static final Logger oLog = LogHelper.getLogger(ComputeEngine.class.getName());

    /**
     * Be sure to specify the name of your application. If the application name is {@code null} or
     * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
     */
    private static final String APPLICATION_NAME = "";

    /** Set PROJECT_ID to your Project ID from the Overview pane in the Developers console. */
    private static final String PROJECT_ID = "vivekshresta-bandaru";

    /** Set Compute Engine zone. */
    private static final String ZONE_NAME = "us-central1-a";

    /** Set the name of the sample VM instance to be created. */
    private static final String SAMPLE_INSTANCE_NAME = "mapper";

    /** Set the path of the OS image for the sample VM instance to be created. */
    private static final String SOURCE_IMAGE_PREFIX =
            "https://www.googleapis.com/compute/v1/projects/";
    //TODO: Change this
    private static final String SOURCE_IMAGE_PATH =
            "vivekshresta-bandaru/global/images/cloud-image";

    /** Set the Network configuration values of the sample VM instance to be created. */
    private static final String NETWORK_INTERFACE_CONFIG = "ONE_TO_ONE_NAT";

    private static final String NETWORK_ACCESS_CONFIG = "External NAT";

    private static final String BUCKET_ID = "vivekshresta-bandaru";

    /** Set the time out limit for operation calls to the Compute Engine API. */
    public static final long OPERATION_TIMEOUT_MILLIS = 60 * 1000;

    /** Global instance of the HTTP transport. */
    private static HttpTransport httpTransport;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    public static Compute getComputeEngine() {
        Compute compute = null;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();

            // Authenticate using Google Application Default Credentials.
            //GoogleCredentials credential = GoogleCredentials.getApplicationDefault();
            String usingSystemProperty = System.getProperty("user.dir");
            String credentialPath = usingSystemProperty + "/user-programs/src/main/resources/credentials/vivekshresta-bandaru-0e6764c75944.json";
            GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(credentialPath))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

            if (credential.createScopedRequired()) {
                List<String> scopes = new ArrayList<>();
                // Set Google Cloud Storage scope to Full Control.
                scopes.add(ComputeScopes.DEVSTORAGE_FULL_CONTROL);
                // Set Google Compute Engine scope to Read-write.
                scopes.add(ComputeScopes.COMPUTE);
                credential = credential.createScoped(scopes);
            }
            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credential);
            // Create Compute Engine object for listing instances.
            compute = new Compute.Builder(httpTransport, JSON_FACTORY, requestInitializer)
                            .setApplicationName(APPLICATION_NAME)
                            .build();
        } catch (Exception e) {
            oLog.warning(e.getMessage());
        } catch (Throwable t) {
            oLog.warning(Arrays.toString(t.getStackTrace()));
        }

        return compute;
    }

    // [START list_instances]
    /**
     * Print available machine instances.
     *
     * @param compute The main API access point
     * @return {@code true} if the instance created by this sample app is in the list
     */
    public static boolean printInstances(Compute compute,String instanceName) throws IOException {
        System.out.println("================== Listing Compute Engine Instances ==================");
        Compute.Instances.List instances = compute.instances().list(PROJECT_ID, ZONE_NAME);
        InstanceList list = instances.execute();
        boolean found = false;
        if (list.getItems() == null) {
            System.out.println(
                    "No instances found. Sign in to the Google Developers Console and create "
                            + "an instance at: https://console.developers.google.com/");
        } else {
            for (Instance instance : list.getItems()) {
                System.out.println(instance.toPrettyString());
                if (instance.getName().equals(instanceName)) {
                    found = true;
                }
            }
        }
        return found;
    }
    // [END list_instances]

    // [START create_instances]
    public static Operation startInstance(Compute compute, String instanceName) throws IOException {
        System.out.println("================== Starting New Instance ==================");

        // Create VM Instance object with the required properties.
        Instance instance = new Instance();
        instance.setName(instanceName);
        instance.setMachineType(
                String.format(
                        "https://www.googleapis.com/compute/v1/projects/%s/zones/%s/machineTypes/custom-2-5120",
                        PROJECT_ID, ZONE_NAME));
        // Add Network Interface to be used by VM Instance.
        NetworkInterface ifc = new NetworkInterface();
        ifc.setNetwork(
                String.format(
                        "https://www.googleapis.com/compute/v1/projects/%s/global/networks/default",
                        PROJECT_ID));
        List<AccessConfig> configs = new ArrayList<>();
        AccessConfig config = new AccessConfig();
        config.setType(NETWORK_INTERFACE_CONFIG);
        config.setName(NETWORK_ACCESS_CONFIG);
        configs.add(config);
        ifc.setAccessConfigs(configs);
        instance.setNetworkInterfaces(Collections.singletonList(ifc));

        // Add attached Persistent Disk to be used by VM Instance.
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        disk.setType("PERSISTENT");
        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
        // Assign the Persistent Disk the same name as the VM Instance.
        params.setDiskName(instanceName);
        // Specify the source operating system machine image to be used by the VM Instance.
        params.setSourceImage(SOURCE_IMAGE_PREFIX + SOURCE_IMAGE_PATH);
        // Specify the disk type as Standard Persistent Disk
        params.setDiskType(
                String.format(
                        "https://www.googleapis.com/compute/v1/projects/%s/zones/%s/diskTypes/pd-standard",
                        PROJECT_ID, ZONE_NAME));
        disk.setInitializeParams(params);
        instance.setDisks(Collections.singletonList(disk));

        // Initialize the service account to be used by the VM Instance and set the API access scopes.
        ServiceAccount account = new ServiceAccount();
        //account.setEmail("vivekshresta@vivekshresta-bandaru.iam.gserviceaccount.com");
        account.setEmail("88735001475-compute@developer.gserviceaccount.com");
        List<String> scopes = new ArrayList<>();
        scopes.add("https://www.googleapis.com/auth/devstorage.full_control");
        scopes.add("https://www.googleapis.com/auth/compute");
        account.setScopes(scopes);
        instance.setServiceAccounts(Collections.singletonList(account));

        // Optional - Add a startup script to be used by the VM Instance.
        // If you put a script called "vm-startup.sh" in this Google Cloud Storage
        // bucket, it will execute on VM startup.  This assumes you've created a
        // bucket named the same as your PROJECT_ID.
        // For info on creating buckets see:
        // https://cloud.google.com/storage/docs/cloud-console#_creatingbuckets
        Metadata meta = new Metadata();
        List<Metadata.Items> items = new ArrayList<>();

        String usingSystemProperty = System.getProperty("user.dir");
        String initPath = usingSystemProperty + "/user-programs/src/main/resources/scripts/init.sh";
        //initPath = String.format("gs://%s/anit.sh", BUCKET_ID);
        items.add(getItem("startup-script", getScriptData(initPath)));
        items.add(getItem("url", "http://storage.googleapis.com/gce-demo-input/photo.jpg"));
        items.add(getItem("text", "Ready for dessert?"));
        items.add(getItem("bucket", BUCKET_ID));
        meta.setItems(items);
        instance.setMetadata(meta);

        System.out.println(instance.toPrettyString());
        Compute.Instances.Insert insert = compute.instances().insert(PROJECT_ID, ZONE_NAME, instance);
        return insert.execute();
    }
    // [END create_instances]

    private static String getScriptData(String path) {
        StringBuilder sb = new StringBuilder();
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine())
                sb.append(myReader.nextLine()).append("\n");
            myReader.close();
        } catch (FileNotFoundException e) {
            oLog.warning("An error occurred while trying to read from a file.");
        }

        return sb.toString();
    }

    private static Metadata.Items getItem(String key, String value) {
        Metadata.Items item = new Metadata.Items();
        item.setKey(key);
        item.setValue(value);

        return item;
    }

    private static Operation deleteInstance(Compute compute, String instanceName) throws Exception {
        System.out.println(
                "================== Deleting Instance " + instanceName + " ==================");
        Compute.Instances.Delete delete =
                compute.instances().delete(PROJECT_ID, ZONE_NAME, instanceName);
        return delete.execute();
    }

    // [START wait_until_complete]
    /**
     * Wait until {@code operation} is completed.
     *
     * @param compute the {@code Compute} object
     * @param operation the operation returned by the original request
     * @param timeout the timeout, in millis
     * @return the error, if any, else {@code null} if there was no error
     * @throws InterruptedException if we timed out waiting for the operation to complete
     * @throws IOException if we had trouble connecting
     */
    public static Operation.Error blockUntilComplete(
            Compute compute, Operation operation, long timeout) throws Exception {
        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000;
        String zone = operation.getZone(); // null for global/regional operations
        if (zone != null) {
            String[] bits = zone.split("/");
            zone = bits[bits.length - 1];
        }
        String status = operation.getStatus();
        String opId = operation.getName();
        while (operation != null && !status.equals("DONE")) {
            Thread.sleep(pollInterval);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= timeout) {
                throw new InterruptedException("Timed out waiting for operation to complete");
            }
            System.out.println("waiting...");
            if (zone != null) {
                Compute.ZoneOperations.Get get = compute.zoneOperations().get(PROJECT_ID, zone, opId);
                operation = get.execute();
            } else {
                Compute.GlobalOperations.Get get = compute.globalOperations().get(PROJECT_ID, opId);
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
