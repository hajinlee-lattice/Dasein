package com.latticeengines.common.exposed.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

public class HttpUtils {
    private static String parseHttpResponse(HttpResponse response) throws IllegalStateException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

        String output = "";
        StringBuilder responseMessage = new StringBuilder("");
        while ((output = br.readLine()) != null) {
            responseMessage.append(output);
        }

        return responseMessage.toString();
    }

    public static String sendGetRequest(String requestUrl, List<BasicNameValuePair> headers, BasicNameValuePair... params)
            throws ClientProtocolException, IOException {

        StringBuilder parameterizedRequestUrl = new StringBuilder(requestUrl);

        String querystring = URLEncodedUtils.format(Arrays.asList(params), "utf-8");
        parameterizedRequestUrl.append("?");
        parameterizedRequestUrl.append(querystring);

        HttpGet httpGet = new HttpGet(parameterizedRequestUrl.toString());
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpGet.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpResponse response = httpclient.execute(httpGet);

        return parseHttpResponse(response);
    }

    public static String sendPostRequest(String requestUrl, List<BasicNameValuePair> headers, String payload)
            throws ClientProtocolException, IOException {
        HttpPost httpPost = new HttpPost(requestUrl);
        for (BasicNameValuePair basicNameValuePair : headers) {
            httpPost.setHeader(basicNameValuePair.getName(), basicNameValuePair.getValue());
        }

        httpPost.setEntity(new StringEntity(payload));

        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpResponse response = httpclient.execute(httpPost);

        return parseHttpResponse(response);
    }

}
