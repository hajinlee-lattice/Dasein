package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.api.client.http.ByteArrayContent;
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

@Deprecated
public class HttpWithRetryUtils {
    static HttpTransport HTTPS_TRANSPORT = null;
    static HttpTransport HTTP_TRANSPORT = null;
    static HttpRequestFactory requestFactory;
    static HttpRequestFactory requestSslFactory;

    static {

        try {
            HTTP_TRANSPORT = new NetHttpTransport();
            HTTPS_TRANSPORT = new NetHttpTransport.Builder().doNotValidateCertificate().build();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
        HttpRequestInitializer initializer = new HttpRequestInitializer() {

            @Override
            public void initialize(HttpRequest request) throws IOException {
                request.setConnectTimeout(60000);
                request.setNumberOfRetries(10);
                request.setReadTimeout(300000);

                ExponentialBackOff backoff = new ExponentialBackOff.Builder().setInitialIntervalMillis(1000)
                        .setMultiplier(2).setMaxElapsedTimeMillis(180000).build();
                request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
            }
        };
        requestFactory = HTTP_TRANSPORT.createRequestFactory(initializer);
        requestSslFactory = HTTPS_TRANSPORT.createRequestFactory(initializer);
    }

    private static HttpRequestFactory getRequestFactory(String url) {
        if (StringUtils.isEmpty(url)) {
            throw new RuntimeException("Url cannot be empty.");
        }
        if (url.startsWith("https:")) {
            return requestSslFactory;
        }
        return requestFactory;
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
        HttpRequest request = getRequestFactory(url).buildPostRequest(new GenericUrl(url),
                ByteArrayContent.fromString("application/json", jsonString));
        request.setParser(new JacksonFactory().createJsonObjectParser());

        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.getHeaders().set(entry.getKey(), entry.getValue());
            }
        }
        // Since the request data is json, the expected response is also json.
        request.getHeaders().setAccept("application/json;");
        HttpResponse response = request.execute();
        String value = IOUtils.toString(response.getContent());

        return value;
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
