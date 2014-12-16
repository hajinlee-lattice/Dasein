package com.latticeengines.marketoharness;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;



// TODO: Move hard-coded refs to config
public final class MarketoUtilities {
	
	public static final String MARKETO_ACCESS_TOKEN_KEY = "access_token";

	public MarketoUtilities() {
	}
	
	public static String getRestIdentityUrl() {
		return "https://976-KKC-431.mktorest.com/identity";
	}

	public static String getRestEndpointUrl() {
		return "https://976-KKC-431.mktorest.com/rest";
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
	
	public static Request getMarketoPostRequest(
			String accessToken,
			String objectEndpoint,
			String bodyJson) {
		Request toReturn = getMarketoObjectRequest(
				accessToken,
				objectEndpoint,
				true);
		toReturn.bodyString(bodyJson, ContentType.APPLICATION_JSON);
		
		return toReturn;
	}
	
	public static Request getMarketoObjectRequest(
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
	
	public static Response insertMarketoLeads(
			String accessToken,
			ArrayList<HashMap<String,String>> leads) throws Exception {

		JSONObject json = formatObjectAction(leads, "createOnly");
		Request request = getMarketoPostRequest(
				accessToken,
				getLeadEndpoint(),
				json.toJSONString());
		
		try {
			Response toReturn = request.execute();
			//TODO: parse/return toReturn.returnContent()
			return toReturn;
		} catch (Exception e) {
			throw new Exception("Error making post request to insert Marketo Leads: " + e.getMessage());
		}
	}

	private static JSONObject formatObjectAction(
			ArrayList<HashMap<String, String>> objects, 
			String action)
			throws IllegalArgumentException {

		if(objects == null || objects.isEmpty())
			throw new IllegalArgumentException("Must specify at least one lead.");
		
		JSONObject toReturn = new JSONObject();
		toReturn.put("action", action);
		JSONArray jsonObjects = new JSONArray();
		toReturn.put("input", jsonObjects);
		
		for(HashMap<String,String> lead:objects) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.putAll(lead);
			jsonObjects.add(jsonObject);
		}
		return toReturn;
	}
	
}
