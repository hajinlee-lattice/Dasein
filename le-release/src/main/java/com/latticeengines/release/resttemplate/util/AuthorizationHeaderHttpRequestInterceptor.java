package com.latticeengines.release.resttemplate.util;

import java.io.IOException;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;

public class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private String headerValue;

    public AuthorizationHeaderHttpRequestInterceptor(String headerValue) {
        this.headerValue = headerValue;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
        requestWrapper.getHeaders().add("Authorization", "Basic " + headerValue);
        requestWrapper.getHeaders().add("Content-Type", "application/json");
        return execution.execute(requestWrapper, body);
    }
}
