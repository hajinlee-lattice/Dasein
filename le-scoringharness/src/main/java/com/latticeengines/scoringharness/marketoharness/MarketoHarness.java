package com.latticeengines.scoringharness.marketoharness;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.scoringharness.cloudmodel.BaseCloudRead;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudUpdate;

// TODO: Replace org.json.simple with fw that properly supports generics
@Service
public class MarketoHarness {

    @Autowired
    private MarketoProperties properties;

    public static final String MARKETO_ACCESS_TOKEN_KEY = "access_token";

    // Object Types
    public static final String OBJECT_TYPE_LEAD = "lead";

    // Object Actions
    public static final String OBJECT_ACTION_CREATE_ONLY = "createOnly";
    public static final String OBJECT_ACTION_UPDATE_ONLY = "updateOnly";
    public static final String OBJECT_ACTION_CREATE_OR_UPDATE = "createOrUpdate";
    public static final String OBJECT_ACTION_CREATE_DUPLICATE = "createDuplicate";

    private MarketoHarness() {
    }

    public String getRestIdentityUrl() {
        return "https://976-KKC-431.mktorest.com/identity";
    }

    public String getRestEndpointUrl() {
        return "https://976-KKC-431.mktorest.com/rest";
    }

    public String getObjectEndpoint(String objectType) throws IllegalArgumentException {
        if (objectType == OBJECT_TYPE_LEAD)
            return "/v1/leads.json";
        else
            throw new IllegalArgumentException("The object type [" + objectType + "] is not supported for Marketo.");
    }

    public String getObjectEndpointForId(String objectType, String id) throws IllegalArgumentException {
        if (objectType == OBJECT_TYPE_LEAD)
            return "/v1/lead/" + id + ".json";
        else
            throw new IllegalArgumentException("The object type [" + objectType + "] is not supported for Marketo.");
    }

    public String getAccessToken() throws Exception {
        return getAccessToken(getRestIdentityUrl(), properties.getClientId(), properties.getClientSecret());
    }

    public String getAccessToken(String identityServiceUrl, String customServiceClientID,
            String customServiceClientSecret) throws Exception {

        String toReturn = null;
        String accessTokenUrl = getAccessTokenUrl(identityServiceUrl, customServiceClientID, customServiceClientSecret);

        Request request = Request.Get(accessTokenUrl);
        Response response = null;
        try {
            response = request.execute();
        } catch (Exception e) {
            throw new Exception("Error obtaining Marketo access token: " + e.getMessage());
        }

        if (response == null)
            throw new Exception("Response from Marketo access token request was null.");

        Content content = response.returnContent();

        if (content == null)
            throw new Exception("Response content from Marketo access token request was null.");

        toReturn = parseAccessToken(content.asString());

        if (toReturn == null || toReturn.trim().isEmpty())
            throw new Exception("Failed to obtain a non-trivial value for Marketo access token.");

        return toReturn;
    }

    private String parseAccessToken(String responseBody) throws Exception {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject) parser.parse(responseBody);
        } catch (ParseException e) {
            throw new Exception("Failed to parse access token from Marketo response: " + e.getMessage());
        }

        if (jsonObject == null)
            throw new Exception("Failed to parse a non-null json object from Marketo token response.");

        return (String) jsonObject.get(MARKETO_ACCESS_TOKEN_KEY);
    }

    public String getAccessTokenUrl(String identityServiceUrl, String customServiceClientID,
            String customServiceClientSecret) {

        return identityServiceUrl + "/oauth/token?grant_type=client_credentials&client_id=" + customServiceClientID
                + "&client_secret=" + customServiceClientSecret;
    }

    public Request getPostRequest(String accessToken, String objectEndpoint, String bodyJson) {
        Request toReturn = getObjectRequest(accessToken, objectEndpoint, true);
        toReturn.bodyString(bodyJson, ContentType.APPLICATION_JSON);

        return toReturn;
    }

    public Request getObjectRequest(String accessToken, String objectEndpoint, boolean isPost) {
        String objectEndpointUrl = getRestEndpointUrl() + objectEndpoint;
        Request toReturn = isPost ? Request.Post(objectEndpointUrl) : Request.Get(objectEndpointUrl);

        toReturn.addHeader("Authorization", "Bearer " + accessToken);
        return toReturn;
    }

    public BaseCloudResult getObjects(String accessToken, BaseCloudRead read) throws Exception {
        if (read == null)
            throw new Exception("Must specify a non-trivial value for getObject() read parameter.");

        if (read.ids.size() == 0)
            throw new Exception("Must specify an object to read.");

        if (read.ids.size() != 1)
            throw new Exception("Objects are currently only allowed to be read one at a time.");

        String endpoint = getObjectEndpointForId(read.objectType, read.ids.get(0));
        if (read.fields != null) {
            endpoint += "?fields=" + StringUtils.join(read.fields, ",");
        }

        Request request = getObjectRequest(accessToken, endpoint, false);

        Response response;
        try {
            response = request.execute();
        } catch (Exception e) {
            throw new Exception("Error making post request to read Marketo objects: " + e.getMessage());
        }

        if (response == null)
            throw new Exception("A null response was returned from the Marketo object read request.");

        Content content = response.returnContent();
        if (content == null) {
            throw new Exception("A null content object was returned from the Marketo object update request.");
        }

        return parseResult(content.asString());
    }

    public BaseCloudResult updateObjects(String accessToken, BaseCloudUpdate update) throws Exception {
        if (update == null)
            throw new Exception("Must specify a non-trivial value for updateObjects() update parameter.");
        JSONObject json = formatObjectUpdate(update);
        Request request = getPostRequest(accessToken, getObjectEndpoint(update.objectType), json.toJSONString());

        Response response;
        try {
            response = request.execute();
        } catch (Exception e) {
            throw new Exception("Error making post request to insert Marketo objects: " + e.getMessage());
        }

        if (response == null)
            throw new Exception("A null response was returned from the Marketo object update request.");

        Content content = response.returnContent();
        if (content == null) {
            throw new Exception("A null content object was returned from the Marketo object update request.");
        }

        return parseResult(content.asString());
    }

    private JSONObject formatObjectUpdate(BaseCloudUpdate update) throws Exception {
        if (update.jsonObjects == null || update.jsonObjects.size() == 0)
            throw new IllegalArgumentException("formatObjectUpdate() requires at least one row to update.");

        JSONObject toReturn = new JSONObject();
        toReturn.put("action", update.action);
        JSONArray jsonObjects = new JSONArray();
        toReturn.put("input", jsonObjects);
        JSONParser parser = new JSONParser();
        for (String jsonObjectString : update.jsonObjects) {
            try {
                jsonObjects.add((JSONObject) parser.parse(jsonObjectString));
            } catch (ParseException e) {
                throw new Exception("formatObjectUpdate() failed to parse a json string object definition.");
            }
        }

        return toReturn;
    }

    private BaseCloudResult parseResult(String responseBody) throws Exception {
        JSONParser parser = new JSONParser();
        JSONObject json;
        try {
            json = (JSONObject) parser.parse(responseBody);
        } catch (ParseException e) {
            throw new Exception("parseUpdateResult() failed to parse the response body to json.");
        }
        String requestId = (String) json.get("requestId");
        Boolean isSuccess = (Boolean) json.get("success");

        BaseCloudResult toReturn;
        if (isSuccess) {
            JSONArray jsonArray = (JSONArray) json.get("result");
            ArrayList<String> jsonObjectResults = new ArrayList<String>();
            if (jsonArray != null) {
                for (Object jsonRowObject : jsonArray.toArray()) {
                    JSONObject jsonRow = (JSONObject) jsonRowObject;
                    jsonObjectResults.add(jsonRow.toJSONString());
                }
            }
            toReturn = new BaseCloudResult(isSuccess, requestId, jsonObjectResults);
        } else {
            JSONArray jsonArray = (JSONArray) json.get("errors");
            String errors = "";
            String separator = "";
            if (jsonArray != null) {
                for (Object jsonRowObject : jsonArray.toArray()) {
                    JSONObject jsonRow = (JSONObject) jsonRowObject;
                    errors += separator + (String) jsonRow.get("code") + ": " + (String) jsonRow.get("message");
                    separator = "; ";
                }
            }
            toReturn = new BaseCloudResult(isSuccess, requestId, errors);
        }

        return toReturn;
    }

    private JSONObject formatObjectRead(BaseCloudRead read) throws Exception {
        JSONObject toReturn = new JSONObject();
        toReturn.put("id", read.ids.get(0));
        if (read.fields.size() > 0) {
            toReturn.put("fields", StringUtils.join(read.fields, ","));
        }
        toReturn.put("_method", "POST");

        return toReturn;
    }
}
