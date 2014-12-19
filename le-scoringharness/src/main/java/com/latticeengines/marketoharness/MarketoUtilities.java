package com.latticeengines.marketoharness;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.latticeengines.cloudmodel.BaseCloudResult;
import com.latticeengines.cloudmodel.BaseCloudUpdate;

import java.util.*;


// TODO: Move hard-coded refs to config
// TODO: Replace org.json.simple with fw that properly supports generics
public final class MarketoUtilities {
	
	public static final String MARKETO_ACCESS_TOKEN_KEY = "access_token";

	// Object Types
	public static final String OBJECT_TYPE_LEAD = "lead";
	
	// Object Actions
	public static final String OBJECT_ACTION_CREATE_ONLY = "createOnly";
	public static final String OBJECT_ACTION_UPDATE_ONLY = "updateOnly";
	public static final String OBJECT_ACTION_CREATE_OR_UPDATE = "createOrUpdate";
	public static final String OBJECT_ACTION_CREATE_DUPLICATE = "createDuplicate";
	
	public MarketoUtilities() {
	}
	
	public static String getRestIdentityUrl() {
		return "https://976-KKC-431.mktorest.com/identity";
	}

	public static String getRestEndpointUrl() {
		return "https://976-KKC-431.mktorest.com/rest";
	}

	public static String getObjectEndpoint(String objectType) throws IllegalArgumentException {
		if(objectType == OBJECT_TYPE_LEAD)
			return getLeadEndpoint();
		else
			throw new IllegalArgumentException("The object type [" + objectType + "] is not supported for Marketo.");
	}
	
	public static String getLeadEndpoint() {
		return "/v1/leads.json";
	}

	public static String getCustomServiceClientID() {
		return "868c37ad-905c-4562-be86-c6b1f39293f4";
	}

	public static String getCustomServiceClientSecret() {
		return "vBt3ZnFAU4eCyrtzOzRZfvkRQPfdDrUi";
	}

	public static String getLeadEndpointUrl() {
		return getRestEndpointUrl() + getLeadEndpoint();
	}
	
	public static String getAccessToken() throws Exception {
		return getAccessToken(
				getRestIdentityUrl(),
				getCustomServiceClientID(),
				getCustomServiceClientSecret());
	}
	
	public static String getAccessToken(
			String identityServiceUrl,
			String customServiceClientID,
			String customServiceClientSecret) throws Exception {
		
		String toReturn = null;
		String accessTokenUrl = getAccessTokenUrl(
				identityServiceUrl,
				customServiceClientID,
				customServiceClientSecret);
				
		Request request = Request.Get(accessTokenUrl);
		Response response = null;
		try {
			response = request.execute();
		} catch (Exception e) {
			throw new Exception("Error obtaining Marketo access token: " + e.getMessage());
		}
		
		if(response == null)
			throw new Exception("Response from Marketo access token request was null.");
		
		Content content = response.returnContent();
		
		if(content == null)
			throw new Exception("Response content from Marketo access token request was null.");
		
		toReturn = parseAccessToken(content.asString());

		if(toReturn == null || toReturn.trim().isEmpty())
			throw new Exception("Failed to obtain a non-trivial value for Marketo access token.");

		return toReturn;
	}
	
	private static String parseAccessToken(String responseBody) throws Exception {
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = null;
		try {
			jsonObject = (JSONObject) parser.parse(responseBody);
		} catch (ParseException e) {
			throw new Exception("Failed to parse access token from Marketo response: " + e.getMessage());
		} 
		
		if(jsonObject == null)
			throw new Exception("Failed to parse a non-null json object from Marketo token response.");
		
		return (String) jsonObject.get(MARKETO_ACCESS_TOKEN_KEY);
	}

	public static String getAccessTokenUrl(
			String identityServiceUrl,
			String customServiceClientID,
			String customServiceClientSecret) {
		
		return identityServiceUrl + 
				"/oauth/token?grant_type=client_credentials&client_id=" +
				customServiceClientID +
				"&client_secret=" +
				customServiceClientSecret;
	}
	
	public static Request getPostRequest(
			String accessToken,
			String objectEndpoint,
			String bodyJson) {
		Request toReturn = getObjectRequest(
				accessToken,
				objectEndpoint,
				true);
		toReturn.bodyString(bodyJson, ContentType.APPLICATION_JSON);
		
		return toReturn;
	}
	
	public static Request getObjectRequest(
			String accessToken,
			String objectEndpoint,
			boolean isPost) {
		String objectEndpointUrl = getRestEndpointUrl() + objectEndpoint;
		Request toReturn = isPost ?
				Request.Post(objectEndpointUrl) :
				Request.Get(objectEndpointUrl);
				
		toReturn.addHeader("Authorization", "Bearer " + accessToken);
		return toReturn;
	}
	
	public static BaseCloudResult updateObjects(
			String accessToken,
			BaseCloudUpdate update) throws Exception {
		if(update == null)
			throw new Exception("Must specify a non-trivial value for updateObjects() update parameter.");
		JSONObject json = formatObjectUpdate(update);
		Request request = getPostRequest(
				accessToken,
				getObjectEndpoint(update.objectType),
				json.toJSONString());
		
		Response response;
		try {
			response = request.execute();
		} catch (Exception e) {
			throw new Exception("Error making post request to insert Marketo objects: " + e.getMessage());
		}
		
		if(response == null)
			throw new Exception("A null response was returned from the Marketo object update request.");
		
        Content content = response.returnContent();
        if (content == null) {
			throw new Exception("A null content object was returned from the Marketo object update request.");
        }

        return parseUpdateResult(
        		content.asString(),
        		update);
	}
	
	private static JSONObject formatObjectUpdate(
			BaseCloudUpdate update)
			throws Exception {
		if(update.jsonObjects == null || update.jsonObjects.size() == 0)
			throw new IllegalArgumentException("formatObjectUpdate() requires at least one row to update.");
		
		JSONObject toReturn = new JSONObject();
		toReturn.put("action", update.action);
		JSONArray jsonObjects = new JSONArray();
		toReturn.put("input", jsonObjects);
		JSONParser parser = new JSONParser();
		for(String jsonObjectString:update.jsonObjects) {
			try {
				jsonObjects.add((JSONObject)parser.parse(jsonObjectString));
			} catch (ParseException e) {
				throw new Exception("formatObjectUpdate() failed to parse a json string object definition.");
			}
		}

		return toReturn;
	}
	
	private static BaseCloudResult parseUpdateResult(
			String responseBody,
			BaseCloudUpdate update) throws Exception {
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
		if(isSuccess) {
			JSONArray jsonArray = (JSONArray) json.get("result");
			ArrayList<String> jsonObjectResults = new ArrayList<String>();
			if(jsonArray != null) {
				for(Object jsonRowObject:jsonArray.toArray()) {
					JSONObject jsonRow = (JSONObject) jsonRowObject;
					jsonObjectResults.add(jsonRow.toJSONString());
				}
			}
			toReturn = new BaseCloudResult(
					update,
					isSuccess,
					requestId,
					jsonObjectResults);
		} else {
			JSONArray jsonArray = (JSONArray) json.get("errors");
			String errors = "";
			String separator = "";
			if(jsonArray != null) {
				for(Object jsonRowObject:jsonArray.toArray()) {
					JSONObject jsonRow = (JSONObject) jsonRowObject;
					errors += separator + (String) jsonRow.get("code") + ": " + (String) jsonRow.get("message");
					separator = "; ";
				}
			}
			toReturn = new BaseCloudResult(
					update,
					isSuccess,
					requestId,
					errors);
		}
		
		return toReturn;
	}

	
}
