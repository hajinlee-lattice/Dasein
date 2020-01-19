package com.latticeengines.testframework.exposed.rest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.UnknownHttpStatusCodeException;

public class LedpResponseErrorHandler extends DefaultResponseErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(LedpResponseErrorHandler.class);

    private String responseString = null;
    private HttpStatus statusCode = null;

    public String getResponseString() {
        return responseString;
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        statusCode = getHttpStatusCode(response);
        responseString = getResponseBodyAsString(response);
        throw new RuntimeException(statusCode + ": " + responseString);
    }

    protected HttpStatus getHttpStatusCode(ClientHttpResponse response) throws IOException {
        HttpStatus statusCode;
        try {
            statusCode = response.getStatusCode();
        }
        catch (IllegalArgumentException ex) {
            throw new UnknownHttpStatusCodeException(response.getRawStatusCode(),
                    response.getStatusText(), response.getHeaders(), getResponseBody(response), getCharset(response));
        }
        return statusCode;
    }

    protected byte[] getResponseBody(ClientHttpResponse response) {
        try {
            InputStream responseBody = response.getBody();
            return FileCopyUtils.copyToByteArray(responseBody);
        } catch (IOException ex) {
            log.warn("Failed to convert response body to byte array", ex);
        }
        return new byte[0];
    }

    protected Charset getCharset(ClientHttpResponse response) {
        HttpHeaders headers = response.getHeaders();
        MediaType contentType = headers.getContentType();
        return contentType != null ? contentType.getCharset() : null;
    }

    private String getResponseBodyAsString(ClientHttpResponse response) {
        try {
            InputStream responseBody = response.getBody();
            return IOUtils.toString(responseBody, getCharset(response));
        } catch (IOException ex) {
            log.warn("Failed to convert response body to String", ex);
        }
        return null;
    }
}
