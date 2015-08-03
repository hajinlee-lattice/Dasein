package com.latticeengines.release.resttemplate.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

public class LoggingHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    private static final Logger log = LoggerFactory.getLogger(LoggingHttpRequestInterceptor.class);

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {

        ClientHttpResponse response = execution.execute(request, body);

        log(request, body, response);

        return response;
    }

    private void log(HttpRequest request, byte[] body, ClientHttpResponse response) throws IOException {
        File logFile = new File("request-body.log");
        logFile.createNewFile();
        FileOutputStream output = new FileOutputStream(logFile);
        IOUtils.write(body, output);
        log.info(request.getHeaders().toString());
        log.info(request.getURI().toString());
        log.info("****************response:" + response.getHeaders());
        log.info("****************response:" + IOUtils.readLines(response.getBody()));

    }

}
