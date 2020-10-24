package helper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashMap;
import java.util.Map;

public class PostBodyParser {

    public static Map<String, String> populatePostBody(String body) {
        Map<String, String> parameterMap = new HashMap<>();
        try {
            JSONObject userDetails = (JSONObject) new JSONParser().parse(body);
            for(Object key: userDetails.keySet())
                parameterMap.put((String)key, (String)userDetails.get(key));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return parameterMap;
    }
}
