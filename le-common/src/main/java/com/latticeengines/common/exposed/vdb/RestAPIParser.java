package com.latticeengines.common.exposed.vdb;

import java.io.IOException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.latticeengines.common.exposed.util.HttpWithRetryUtils;

public class RestAPIParser {

    public static String getSpecDetails(String url, Map<String, String> headers, Map<String, String> parameters)
            throws Exception {
        String response = getRestAPIResponse(url, headers, parameters);
        JSONObject jsonObject = parseJSON(response);
        return jsonObject.get("SpecDetails").toString();

    }

    public static String getRestAPIResponse(String url, Map<String, String> headers, Map<String, String> parameters)
            throws IOException {
        return HttpWithRetryUtils.executePostRequest(url, parameters, headers);
    }

    private static JSONObject parseJSON(String response) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(response);
        return jsonObject;

    }
}
