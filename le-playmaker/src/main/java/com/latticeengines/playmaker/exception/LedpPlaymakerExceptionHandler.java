package com.latticeengines.playmaker.exception;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@ControllerAdvice
public class LedpPlaymakerExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(LedpPlaymakerExceptionHandler.class);

    @Inject
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    public LedpPlaymakerExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public JsonNode handleException(LedpException ex, HttpServletRequest request) {
        String stackTrace = ex.getCause() != null ? ExceptionUtils.getStackTrace(ex.getCause()) : ExceptionUtils
                .getStackTrace(ex);

        String tenantName = null;
        try {
            tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        } catch (Exception e) {
            tenantName = e.getMessage();
        }
        log.error(String.format("%s tenantName=%s\n%s", ex.getCode(), tenantName, stackTrace));

        String errorMsg = ex.getMessage();
        Throwable cause = ex;
        while (cause != null) {
            if (cause.getCause() != null) {
                errorMsg = cause.getCause().getMessage();
            }
            cause = cause.getCause();
        }

        return JsonUtils.getObjectMapper().valueToTree(ImmutableMap.of("errorCode", ex.getCode().name(), //
                "errorMsg", ex.getCode().getMessage(), "cause", errorMsg));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public Map<String, Object> handleException(Exception ex, HttpServletRequest request) {
        String trace = ExceptionUtils.getStackTrace(ex);
        String tenantName = null;
        try {
            tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        } catch (Exception e) {
            tenantName = e.getMessage();
        }
        log.error(String.format("tenantName=%s\n%s", tenantName, trace));

        List<String> messages = new ArrayList<String>();
        Throwable cause = ex;
        while (cause != null) {
            messages.add(cause.getMessage());
            cause = cause.getCause();
        }

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("summary", ex.getMessage());
        details.put("errors", messages);
        // details.put("trace", trace);

        return details;
    }
}
