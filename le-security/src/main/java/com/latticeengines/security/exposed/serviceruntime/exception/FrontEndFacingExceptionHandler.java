package com.latticeengines.security.exposed.serviceruntime.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public abstract class FrontEndFacingExceptionHandler extends BaseExceptionHandler {

    public FrontEndFacingExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(RemoteLedpException e) {
        logError(e);
        return getModelAndView(e);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        logError(e.getCode() + "\n" + stackTrace);
        return getModelAndView(e);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(Exception e) {
        logError(e);
        triggerCriticalAlert(e);

        return getModelAndView();
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ModelAndView handleException(AccessDeniedException e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        logError(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_18003.name(), //
                "errorMsg", LedpException.buildMessage(LedpCode.LEDP_18003, new String[] {})));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ModelAndView handleException(LoginException e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        logError(stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));
    }

    private ModelAndView getModelAndView() {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", LedpCode.LEDP_00002.getMessage()));
    }

    private ModelAndView getModelAndView(LedpException e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));
    }

}
