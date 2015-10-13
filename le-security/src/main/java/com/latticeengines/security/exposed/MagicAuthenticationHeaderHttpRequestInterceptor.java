package com.latticeengines.security.exposed;

import java.io.IOException;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;

public class MagicAuthenticationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private String headerValue;

    public MagicAuthenticationHeaderHttpRequestInterceptor() {
        this.headerValue = Constants.INTERNAL_SERVICE_HEADERVALUE;
    }

    public MagicAuthenticationHeaderHttpRequestInterceptor(String headerValue) {
        this.headerValue = headerValue;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
        requestWrapper.getHeaders().add(Constants.INTERNAL_SERVICE_HEADERNAME, headerValue);

        return execution.execute(requestWrapper, body);
    }

    public void setAuthValue(String headerValue) {
        this.headerValue = headerValue;
    }
}
