package com.latticeengines.playmaker.exception;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@ControllerAdvice
public class LedpPlaymakerExceptionHandler {
    private static final Log log = LogFactory.getLog(LedpPlaymakerExceptionHandler.class);

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    public LedpPlaymakerExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException ex, HttpServletRequest request) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = ex.getCause() != null ? ExceptionUtils.getFullStackTrace(ex.getCause()) : ExceptionUtils
                .getStackTrace(ex);

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        log.error(String.format("%s tenantName=%s\n%s", ex.getCode(), tenantName, stackTrace));

        String errorMsg = ex.getMessage();
        Throwable cause = ex;
        while (cause != null) {
            if (cause.getCause() != null) {
                errorMsg = cause.getCause().getMessage();
            }
            cause = cause.getCause();
        }

        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", ex.getCode().name(), //
                "errorMsg", ex.getCode().getMessage(), "cause", errorMsg));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(Exception ex, HttpServletRequest request) {
        String trace = ExceptionUtils.getFullStackTrace(ex);
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
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
