package com.latticeengines.pls.end2end;

import java.io.IOException;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;

public class Oauth2HeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private String headerValue;

    public Oauth2HeaderHttpRequestInterceptor(String headerValue) {
        this.headerValue = headerValue;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
        requestWrapper.getHeaders().add("Authorization", "Bearer " + headerValue);
        return execution.execute(requestWrapper, body);
    }
}