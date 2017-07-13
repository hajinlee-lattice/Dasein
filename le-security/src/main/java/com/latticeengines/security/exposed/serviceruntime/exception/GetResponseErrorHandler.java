package com.latticeengines.security.exposed.serviceruntime.exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public class GetResponseErrorHandler implements ResponseErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(GetResponseErrorHandler.class);

    public GetResponseErrorHandler() {
    }

    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return response.getStatusCode() != HttpStatus.OK;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(response.getBody(), baos);
        String body = new String(baos.toByteArray());
        if (!interpretAndThrowException(response.getStatusCode(), response.getHeaders(), body)) {
            log.error("Could not interpret exception response: " + body);
            HttpHeaders httpHeaders = response.getHeaders();
            if (httpHeaders != null) {
                log.info("HTTP Headers: " + JsonUtils.serialize(httpHeaders));
            }
            throw new RuntimeException(body);
        }
    }

    private boolean interpretAndThrowException(HttpStatus status, HttpHeaders httpHeaders, String body) {
        RemoteLedpException exception;
        try {
            JsonNode node = new ObjectMapper().readTree(body);
            JsonNode stackTrace = node.get("stackTrace");
            String stackTraceString = null;
            if (stackTrace != null) {
                stackTraceString = stackTrace.asText();
            }

            if (httpHeaders != null) {
                log.info("HTTP Headers: " + JsonUtils.serialize(httpHeaders));
            }

            LedpCode code = LedpCode.valueOf(node.get("errorCode").asText());
            String message = node.get("errorMsg").asText();

            // TODO: temporary workaround. After finding out the root cause of
            // json truncation, we should remove this.
            if (message.contains("Could not read JSON: Unexpected end-of-input") || (stackTraceString != null
                    && stackTraceString.contains("Could not read JSON: Unexpected end-of-input"))) {
                throw new RuntimeException("Seems JSON IO was truncated: " + body);
            }
            exception = new RemoteLedpException(stackTraceString, status, code, message);
        } catch (Exception e) {
            return false;
        }
        throw exception;
    }
}
