package com.latticeengines.security.exposed.serviceruntime.exception;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@ControllerAdvice
public class SecurityControllerExceptionHandler extends FrontEndFacingExceptionHandler {

    @Override
    @ExceptionHandler
    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ResponseBody
    public JsonNode handleException(AccessDeniedException e) {
        String errorCode = LedpCode.LEDP_18003.name();
        String errorMsg = LedpException.buildMessage(LedpCode.LEDP_18003, new String[] {});
        if (isDropboxCall()) {
            errorMsg = LedpException.buildMessage(LedpCode.LEDP_18210, new String[] {});
        }
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause())
                : ExceptionUtils.getStackTrace(e);
        logError(stackTrace);
        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", errorCode, //
                "errorMsg", errorMsg));
    }

    private boolean isDropboxCall() {
        HttpServletRequest request = getCurrentRequest();
        if (request != null && StringUtils.isNotEmpty(request.getRequestURI())) {
            return request.getRequestURI().equals("/pls/dropbox") && request.getMethod().equalsIgnoreCase("GET");
        }
        return false;
    }
}
