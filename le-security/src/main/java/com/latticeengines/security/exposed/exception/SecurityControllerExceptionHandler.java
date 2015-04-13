package com.latticeengines.security.exposed.exception;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJacksonJsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@ControllerAdvice
public class SecurityControllerExceptionHandler {

    private static final Log log = LogFactory.getLog(SecurityControllerExceptionHandler.class);
    public SecurityControllerExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ModelAndView handleException(AccessDeniedException e) {
        MappingJacksonJsonView jsonView = new MappingJacksonJsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : ExceptionUtils.getStackTrace(e);
        log.error(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_18003.name(), //
                "errorMsg", LedpException.buildMessage(LedpCode.LEDP_18003, new String[] {})));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ModelAndView handleException(LoginException e) {
        MappingJacksonJsonView jsonView = new MappingJacksonJsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : ExceptionUtils.getStackTrace(e);
        log.error(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));

    }

}
