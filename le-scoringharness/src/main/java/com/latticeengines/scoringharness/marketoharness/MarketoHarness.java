package com.latticeengines.scoringharness.marketoharness;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudRead;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudUpdate;
import com.latticeengines.scoringharness.util.JsonUtil;

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
        JsonNode jsonObject = null;
        try {
            jsonObject = JsonUtil.parseObject(responseBody);
        } catch (Exception e) {
            throw new Exception("Failed to parse access token from Marketo response: " + e.getMessage());
        }

        if (jsonObject == null)
            throw new Exception("Failed to parse a non-null json object from Marketo token response.");

        return jsonObject.get(MARKETO_ACCESS_TOKEN_KEY).asText();
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

        Request request = getPostRequest(accessToken, getObjectEndpoint(update.objectType), formatObjectUpdate(update));

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

    private String formatObjectUpdate(BaseCloudUpdate update) throws Exception {
        if (update.objects == null || update.objects.size() == 0)
            throw new IllegalArgumentException("formatObjectUpdate() requires at least one row to update.");

        ObjectNode toReturn = new ObjectMapper().createObjectNode();
        toReturn.put("action", update.action);
        toReturn.set("input", update.objects);
        return toReturn.toString();
    }

    private BaseCloudResult parseResult(String responseBody) throws Exception {
        ObjectNode json;
        try {
            json = JsonUtil.parseObject(responseBody);
        } catch (Exception e) {
            throw new Exception("parseUpdateResult() failed to parse the response body to json.");
        }
        String requestId = json.get("requestId").asText();
        Boolean isSuccess = json.get("success").asBoolean();

        BaseCloudResult toReturn;
        if (isSuccess) {
            toReturn = new BaseCloudResult(isSuccess, requestId, (ArrayNode) json.get("result"));
        } else {
            ArrayNode jsonArray = (ArrayNode) json.get("errors");
            String errors = "";
            String separator = "";
            if (jsonArray != null) {
                for (JsonNode errorJson : jsonArray) {
                    errors += separator + errorJson.get("code").asText() + ": " + errorJson.get("message").asText();
                    separator = "; ";
                }
            }
            toReturn = new BaseCloudResult(isSuccess, requestId, errors);
        }

        return toReturn;
    }
}
