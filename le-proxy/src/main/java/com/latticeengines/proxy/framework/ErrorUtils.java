package com.latticeengines.proxy.framework;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public class ErrorUtils {

    private static final Logger log = LoggerFactory.getLogger(GetResponseErrorHandler.class);

    public static RemoteLedpException handleError(WebClientResponseException webClientException) {
        String body = new String(webClientException.getResponseBodyAsByteArray());
        return interpretAndThrowException(webClientException.getStatusCode(), webClientException.getHeaders(), body);
    }

    private static RemoteLedpException interpretAndThrowException(HttpStatus status, HttpHeaders httpHeaders,
            String body) {
        log.info("HTTP Status: " + status.toString());
        if (httpHeaders != null) {
            log.info("HTTP Headers: " + JsonUtils.serialize(httpHeaders));
        }
        RemoteLedpException exception = null;
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
            exception = new RemoteLedpException(stackTraceString, status, code, message);
        } catch (Exception e) {
            exception = new RemoteLedpException(body, status, LedpCode.LEDP_00007, body);
        }
        throw exception;
    }

    public static String shouldRetryFor(Throwable e, Set<Class<? extends Throwable>> retryExceptions, Set<String> retryMessages) {
        String reason = null;
        Throwable cause = findRetryCause(e, retryExceptions);
        if (cause != null) {
            reason = cause.getClass().getCanonicalName();
        } else if (e instanceof RemoteLedpException) {
            RemoteLedpException remoteLedpException = (RemoteLedpException) e;
            HttpStatus httpStatus = remoteLedpException.getHttpStatus();
            if (HttpStatus.BAD_GATEWAY.equals(httpStatus) || HttpStatus.GATEWAY_TIMEOUT.equals(httpStatus)
                    || HttpStatus.SERVICE_UNAVAILABLE.equals(httpStatus)) {
                reason = "HttpStatus=" + httpStatus;
            } else {
                String stackTrace = remoteLedpException.getRemoteStackTrace();
                if (StringUtils.isNotBlank(stackTrace)) {
                    for (Class<? extends Throwable> c : retryExceptions) {
                        if (stackTrace.contains(c.getCanonicalName())) {
                            reason = c.getCanonicalName();
                            break;
                        }
                    }
                    for (String msg : retryMessages) {
                        if (((RemoteLedpException) e).getRemoteStackTrace().contains(msg)) {
                            reason = msg;
                            break;
                        }
                    }
                }
            }
        }
        return reason;
    }

    private static Throwable findRetryCause(Throwable e, Set<Class<? extends Throwable>> retryExceptions) {
        Throwable reason = null;
        if (e != null) {
            if (retryExceptions.contains(e.getClass())) {
                reason = e;
            } else {
                reason = findRetryCause(e.getCause(), retryExceptions);
            }
        }
        return reason;
    }

}
