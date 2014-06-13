package com.latticeengines.common.exposed.util;

import java.io.IOException;

import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;

public class HttpUtils {
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    static HttpRequestFactory requestFactory;

    static {
        requestFactory = HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {

            @Override
            public void initialize(HttpRequest request) throws IOException {
                request.setEncoding(new GZipEncoding());
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
     * @return Returns the HTTP status code or 0 for none.
     */
    public static int executePostRequest(String url) throws IOException {
        HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(url), new EmptyContent());
        HttpResponse response = request.execute();            
        return response.getStatusCode();
    }
    
    public static int executeGetRequest(String url) throws IOException {
        HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));        
        HttpResponse response = request.execute();
        return response.getStatusCode();
    }

}
