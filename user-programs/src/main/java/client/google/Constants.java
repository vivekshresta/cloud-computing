package client.google;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

public class Constants {
    public static final String PROJECT_ID = "vivekshresta-bandaru";
    public static final String ZONE_NAME = "us-central1-f";
    public static final String APPLICATION_NAME = "cloud-computing";
    public static final long OPERATION_TIMEOUT_MILLIS = 120 * 1000;
    public static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
}
