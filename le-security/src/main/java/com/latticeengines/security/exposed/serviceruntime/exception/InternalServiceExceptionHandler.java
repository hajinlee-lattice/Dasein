package com.latticeengines.security.exposed.serviceruntime.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public abstract class InternalServiceExceptionHandler extends BaseExceptionHandler {

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(RemoteLedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        if (e.getRemoteStackTrace() != null) {
            stackTrace = stackTrace + "\nCaused remotely by...\n" + e.getRemoteStackTrace();
        }
        logError(e);
        return getJsonView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        logError(e);
        return getJsonView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(Exception e) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        if (stackTrace.contains("org.apache.catalina.connector.ClientAbortException")) {
            logWarning(e.getMessage());
        } else {
            logError(e);
            triggerCriticalAlert(e);
        }
        return getJsonView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ResponseBody
    public JsonNode handleException(LoginException e) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        logError(e);
        triggerCriticalAlert(e);
        return getJsonView(e, stackTrace);
    }

    private JsonNode getJsonView(Exception e, String stackTrace) {
        String serialized = JsonUtils.serialize(ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", emptyStringIfNull(e.getMessage()), "stackTrace", stackTrace));
        return JsonUtils.deserialize(serialized, JsonNode.class);
    }

    private JsonNode getJsonView(LedpException e, String stackTrace) {
        String serialized = JsonUtils.serialize(ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", emptyStringIfNull(e.getMessage()), "stackTrace", stackTrace));
        return JsonUtils.deserialize(serialized, JsonNode.class);
    }

    private String emptyStringIfNull(Object o) {
        return o != null ? o.toString() : "";
    }

}
