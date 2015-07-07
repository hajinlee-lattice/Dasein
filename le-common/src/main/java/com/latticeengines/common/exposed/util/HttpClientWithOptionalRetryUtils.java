package com.latticeengines.common.exposed.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

public class HttpClientWithOptionalRetryUtils {
    private static final long INITIAL_WAIT_INTERVAL = 100L;
    private static final long MAX_WAIT_INTERVAL = 60000L;
    private static final int MAX_RETRIES = 12;

    private static String parseHttpResponse(HttpResponse response) throws IllegalStateException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

        String output;
        StringBuilder responseMessage = new StringBuilder("");
        while ((output = br.readLine()) != null) {
            responseMessage.append(output);
        }

        return responseMessage.toString();
    }

    public static String sendGetRequest(String requestUrl, boolean retry, List<BasicNameValuePair> headers,
            BasicNameValuePair... params) throws IOException {

        StringBuilder parameterizedRequestUrl = new StringBuilder(requestUrl);

        String querystring = URLEncodedUtils.format(Arrays.asList(params), "utf-8");
        parameterizedRequestUrl.append("?");
        parameterizedRequestUrl.append(querystring);

        HttpGet httpGet = new HttpGet(parameterizedRequestUrl.toString());
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpGet.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        HttpResponse response = executeHttpClient(retry ? MAX_RETRIES : 0, httpGet);

        return parseHttpResponse(response);
    }

    public static String sendPostRequest(String requestUrl, boolean retry, List<BasicNameValuePair> headers,
            String payload) throws IOException {
        HttpPost httpPost = new HttpPost(requestUrl);
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpPost.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        httpPost.setEntity(new StringEntity(payload));

        HttpResponse response = executeHttpClient(retry ? MAX_RETRIES : 0, httpPost);

        return parseHttpResponse(response);
    }

    public static String sendPutRequest(String requestUrl, boolean retry, List<BasicNameValuePair> headers,
                                         String payload) throws IOException {
        HttpPut httpPut = new HttpPut(requestUrl);
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpPut.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        httpPut.setEntity(new StringEntity(payload));

        HttpResponse response = executeHttpClient(retry ? MAX_RETRIES : 0, httpPut);

        return parseHttpResponse(response);
    }

    public static String sendDeleteRequest(String requestUrl, boolean retry, List<BasicNameValuePair> headers)
            throws IOException {
        HttpDelete httpDelete = new HttpDelete(requestUrl);
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpDelete.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        HttpResponse response = executeHttpClient(retry ? MAX_RETRIES : 0, httpDelete);

        return parseHttpResponse(response);
    }

    private static HttpResponse executeHttpClient(int maxRetries, HttpUriRequest request) throws IOException
             {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        boolean retry = true;
        int retries = 0;
        HttpResponse response = null;
        IOException exception = null;

        do {
            long waitTime = Math.min(getExponentialWaitTime(retries), MAX_WAIT_INTERVAL);
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                // Do nothing if sleep interrupted
            }

            try {
                response = httpclient.execute(request);
                retry = false;
            } catch (IOException e) {
                exception = e;
            }
        } while (retry && (retries++ < maxRetries));

        if (response == null && exception != null) {
            throw exception;
        }

        return response;
    }

    private static long getExponentialWaitTime(int retryCount) {
        long waitTime = retryCount == 0 ? 0 : ((long) Math.pow(2, retryCount) * INITIAL_WAIT_INTERVAL);
        return waitTime;
    }

}
