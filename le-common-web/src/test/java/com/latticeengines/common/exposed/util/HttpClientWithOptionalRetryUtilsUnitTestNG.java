package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

public class HttpClientWithOptionalRetryUtilsUnitTestNG {

    private static final String SOME_TEST_PAYLOAD = "some test payload";
    private static final String _101 = "101";
    private static final String TEMPERATURE = "temperature";
    private static final String TOKEN_SOME_TOKEN = "Token token=someToken";
    private static final String APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String AUTHORIZATION = "Authorization";

    @Test(groups = "unit")
    public void testExecuteGetRequest() throws Exception {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair(AUTHORIZATION, TOKEN_SOME_TOKEN));
        headers.add(new BasicNameValuePair(CONTENT_TYPE, APPLICATION_JSON));

        String result = HttpClientWithOptionalRetryUtils.sendGetRequest("https://httpbin.org/get", false, headers, new BasicNameValuePair(TEMPERATURE, _101));
        confirmGetResponse(result);

        result = HttpClientWithOptionalRetryUtils.sendGetRequest("https://httpbin.org/get", true, headers, new BasicNameValuePair(TEMPERATURE, _101));
        confirmGetResponse(result);
    }

    private void confirmGetResponse(String result) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject resultObj = (JSONObject)parser.parse(result);
        JSONObject argsObj = (JSONObject)resultObj.get("args");
        assertTrue(argsObj.get(TEMPERATURE).equals(_101));

        JSONObject headersObj = (JSONObject)resultObj.get("headers");
        assertTrue(headersObj.get(AUTHORIZATION).equals(TOKEN_SOME_TOKEN));
        assertTrue(headersObj.get(CONTENT_TYPE).equals(APPLICATION_JSON));
    }

    @Test(groups = "unit")
    public void testExecutePostRequest() throws Exception {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair(AUTHORIZATION, TOKEN_SOME_TOKEN));
        headers.add(new BasicNameValuePair(CONTENT_TYPE, APPLICATION_JSON));

        String result = HttpClientWithOptionalRetryUtils.sendPostRequest("https://httpbin.org/post", false, headers, SOME_TEST_PAYLOAD);
        confirmPostResponse(result);
    }

    private void confirmPostResponse(String result) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject resultObj = (JSONObject)parser.parse(result);

        assertTrue(resultObj.get("data").equals(SOME_TEST_PAYLOAD));

        JSONObject headersObj = (JSONObject)resultObj.get("headers");
        assertTrue(headersObj.get(AUTHORIZATION).equals(TOKEN_SOME_TOKEN));
        assertTrue(headersObj.get(CONTENT_TYPE).equals(APPLICATION_JSON));
    }
}
