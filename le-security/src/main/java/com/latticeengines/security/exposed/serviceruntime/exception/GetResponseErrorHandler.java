package com.latticeengines.security.exposed.serviceruntime.exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public class GetResponseErrorHandler implements ResponseErrorHandler {

    private static final Logger log = Logger.getLogger(GetResponseErrorHandler.class);

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
        if (!interpretAndThrowException(response.getStatusCode(), body)) {
            log.error("Could not interpret exception response: " + body);
            throw new RuntimeException(body);
        }
    }

    private boolean interpretAndThrowException(HttpStatus status, String body) {
        RemoteLedpException exception;
        try {
            JsonNode node = new ObjectMapper().readTree(body);
            JsonNode stackTrace = node.get("stackTrace");
            String stackTraceString = null;
            if (stackTrace != null) {
                stackTraceString = stackTrace.asText();
            }

            LedpCode code = LedpCode.valueOf(node.get("errorCode").asText());
            String message = node.get("errorMsg").asText();

            //TODO: temporary workaround. After finding out the root cause of json truncation, we should remove this.
            if (message.contains("Could not read JSON: Unexpected end-of-input in VALUE_STRING")
                    || message.contains("Could not read JSON: Unexpected end-of-input in character escape sequence")
                    || stackTraceString.contains("Could not read JSON: Unexpected end-of-input in VALUE_STRING")
                    || stackTraceString.contains("Could not read JSON: Unexpected end-of-input in character escape sequence")) {
                throw new RuntimeException("Seems JSON IO was truncated: " + body);
            }
            exception = new RemoteLedpException(stackTraceString, status, code, message);
        } catch (Exception e) {
            return false;
        }
        throw exception;
    }
}
