package client;

import helper.PostBodyParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class HttpClient {
    private final String url;

    public HttpClient(String ipAddress, String port, String path) {
        //http://localhost:8080/mapperdata
        StringBuilder sb = new StringBuilder();
        this.url = sb.append("http://").append(ipAddress).append(":").append(port).append("/").append(path).toString();
    }

    public Map<String, String> getDataFromMaster() {
        Map<String, String> postBody = new HashMap<>();
        HttpURLConnection con = null;
        BufferedReader in = null;
        try {
            URL urlObj = new URL(url);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("GET");

            in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null)
                content.append(inputLine);

            postBody = PostBodyParser.populatePostBody(content.toString());
            System.out.println(postBody);

            con.disconnect();
            in.close();
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
}
