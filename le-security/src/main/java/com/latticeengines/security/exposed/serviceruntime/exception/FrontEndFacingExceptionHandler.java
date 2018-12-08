package com.latticeengines.security.exposed.serviceruntime.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
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
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public abstract class FrontEndFacingExceptionHandler extends BaseExceptionHandler {

    public FrontEndFacingExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(RemoteLedpException e) {
        logError(e);
        return getJsonView(e);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause())
                : ExceptionUtils.getStackTrace(e);
        logError(e.getCode() + "\n" + stackTrace);
        return getJsonView(e);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(UIActionException e) {
        logError(e);
        return JsonUtils.getObjectMapper() //
                .valueToTree(ImmutableMap.of(UIAction.class.getSimpleName(), e.getUIAction()));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(Exception e) {
        logError(e);
        triggerCriticalAlert(e);

        return getJsonView();
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ResponseBody
    public JsonNode handleException(AccessDeniedException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause())
                : ExceptionUtils.getStackTrace(e);
        logError(stackTrace);
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", LedpCode.LEDP_18003.name(), //
                "errorMsg", LedpException.buildMessage(LedpCode.LEDP_18003, new String[] {})));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ResponseBody
    public JsonNode handleException(LoginException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause())
                : ExceptionUtils.getStackTrace(e);
        logError(stackTrace);
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));
    }

    private JsonNode getJsonView() {
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", LedpCode.LEDP_00002.getMessage()));
    }

    private JsonNode getJsonView(LedpException e) {
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));
    }

}
