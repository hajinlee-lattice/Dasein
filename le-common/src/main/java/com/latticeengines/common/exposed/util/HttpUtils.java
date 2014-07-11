package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.google.api.client.http.ByteArrayContent;
//import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;

public class HttpUtils {
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static HttpRequestFactory requestFactory;

    static {
        requestFactory = HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {

            @Override
            public void initialize(HttpRequest request) throws IOException {
                // request.setEncoding(new GZipEncoding());
                request.setConnectTimeout(60000);
                request.setNumberOfRetries(10);

                ExponentialBackOff backoff = new ExponentialBackOff.Builder().setInitialIntervalMillis(1000)
                        .setMultiplier(2).setMaxElapsedTimeMillis(180000).build();
                request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
            }
        });

    }

    /*
     * Request will retry with exponential backoff on any 5xx server error
     * response.
     * 
     * @return Returns the response text
     */
    public static String executePostRequest(String url, Object requestData, Map<String, String> headers)
            throws IOException {
        String jsonString = JsonUtils.serialize(requestData);
        HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(url),
                ByteArrayContent.fromString("application/json", jsonString));
        request.setParser(new JacksonFactory().createJsonObjectParser());
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.getHeaders().set(entry.getKey(), entry.getValue());
        }

        HttpResponse response = request.execute();
        return IOUtils.toString(response.getContent());
    }

    /*
     * Request will retry with exponential backoff on any 5xx server error
     * response.
     * 
     * @return Returns the response text
     */
    public static String executeGetRequest(String url) throws IOException {
        HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
        HttpResponse response = request.execute();

        return IOUtils.toString(response.getContent());
    }

}
