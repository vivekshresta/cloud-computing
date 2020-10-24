package client;

import helper.LogHelper;
import helper.PostBodyParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class HttpClient {
    private static final Logger oLog = LogHelper.getLogger(HttpClient.class.getName());
    private final String url;

    public HttpClient(String ipAddress, String port) {
        //http://localhost:8080/mapperdata
        StringBuilder sb = new StringBuilder();
        this.url = sb.append("http://").append(ipAddress).append(":").append(port).append("/").toString();
    }

    public Map<String, String> getDataFromMaster(String path) {
        Map<String, String> postBody = new HashMap<>();
        HttpURLConnection con = null;
        BufferedReader in = null;
        try {
            URL urlObj = new URL(url + path);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("GET");

            in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null)
                content.append(inputLine);

            postBody = PostBodyParser.populatePostBody(content.toString());
            oLog.info("Data from master: " + postBody.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null)
                    in.close();
                if(con != null)
                    con.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return postBody;
    }

    public void intimateMasterAboutCompletion(String path) {
        HttpURLConnection con = null;
        try {
            URL urlObj = new URL(url + path);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("GET");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(con != null)
                con.disconnect();
        }
    }
}
