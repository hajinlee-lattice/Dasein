package com.latticeengines.security.exposed.serviceruntime.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;

@ControllerAdvice
public class SecurityControllerExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SecurityControllerExceptionHandler.class);

    public SecurityControllerExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ModelAndView handleException(AccessDeniedException e) {

        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        log.error(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_18003.name(), //
                "errorMsg", LedpException.buildMessage(LedpCode.LEDP_18003, new String[] {})));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ModelAndView handleException(LoginException e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        log.error(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));

    }
}
