package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpClientWithOptionalRetryUtilsUnitTestNG {

    private static final String SOME_TEST_PAYLOAD = "some test payload";
    private static final String _101 = "101";
    private static final String TEMPERATURE = "temperature";
    private static final String TOKEN_SOME_TOKEN = "Token token=someToken";
    private static final String APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String AUTHORIZATION = "Authorization";

    @SuppressWarnings("deprecation")
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

    private void confirmGetResponse(String result) throws IOException {
        ObjectMapper parser = new ObjectMapper();
        JsonNode resultObj = parser.readTree(result);
        JsonNode argsObj = resultObj.get("args");
        Assert.assertTrue(argsObj.get(TEMPERATURE).asText().equals(_101));

        JsonNode headersObj = resultObj.get("headers");
        Assert.assertTrue(headersObj.get(AUTHORIZATION).asText().equals(TOKEN_SOME_TOKEN));
        Assert.assertTrue(headersObj.get(CONTENT_TYPE).asText().equals(APPLICATION_JSON));
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void testExecutePostRequest() throws Exception {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair(AUTHORIZATION, TOKEN_SOME_TOKEN));
        headers.add(new BasicNameValuePair(CONTENT_TYPE, APPLICATION_JSON));

        String result = HttpClientWithOptionalRetryUtils.sendPostRequest("https://httpbin.org/post", false, headers, SOME_TEST_PAYLOAD);
        confirmPostResponse(result);
    }

    private void confirmPostResponse(String result) throws IOException {
        ObjectMapper parser = new ObjectMapper();
        JsonNode resultObj = parser.readTree(result);

        Assert.assertTrue(resultObj.get("data").asText().equals(SOME_TEST_PAYLOAD));

        JsonNode headersObj = resultObj.get("headers");
        Assert.assertTrue(headersObj.get(AUTHORIZATION).asText().equals(TOKEN_SOME_TOKEN));
        Assert.assertTrue(headersObj.get(CONTENT_TYPE).asText().equals(APPLICATION_JSON));
    }
}
