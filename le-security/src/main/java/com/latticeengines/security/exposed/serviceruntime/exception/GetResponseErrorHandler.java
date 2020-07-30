package com.latticeengines.security.exposed.serviceruntime.exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.core.type.TypeReference;
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
            throw new RemoteLedpException(body, response.getStatusCode(), LedpCode.LEDP_00007);
        }
    }

    private boolean interpretAndThrowException(HttpStatus status, HttpHeaders httpHeaders, String body) {
        RemoteLedpException exception;
        log.info("HTTP Status: " + status.toString());
        if (httpHeaders != null) {
            log.info("HTTP Headers: " + JsonUtils.serialize(httpHeaders));
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = objectMapper.readTree(body);
            JsonNode stackTrace = node.get("stackTrace");
            String stackTraceString = null;
            if (stackTrace != null) {
                stackTraceString = stackTrace.asText();
            }
            LedpCode code = LedpCode.valueOf(node.get("errorCode").asText());
            JsonNode errorParamsMap = node.get("errorParamsMap");
            String message = node.get("errorMsg").asText();
            Map<String, Object> map = null;
            if (errorParamsMap != null) {
                map = objectMapper.convertValue(errorParamsMap, new TypeReference<Map<String, Object>>() {});
            }
            exception = new RemoteLedpException(stackTraceString, status, code, message, map);
        } catch (Exception e) {
            log.warn("Failed parse remote exception body " + body);
            return false;
        }
        throw exception;
    }
}
