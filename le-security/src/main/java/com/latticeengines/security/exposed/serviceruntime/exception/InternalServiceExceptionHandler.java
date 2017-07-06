package com.latticeengines.security.exposed.serviceruntime.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.LoginException;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;

public abstract class InternalServiceExceptionHandler extends BaseExceptionHandler {

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(RemoteLedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        if (e.getRemoteStackTrace() != null) {
            stackTrace = stackTrace + "\nCaused remotely by...\n" + e.getRemoteStackTrace();
        }
        logError(e);
        return getModelAndView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException e) {
        String stackTrace = e.getCause() != null ? ExceptionUtils.getStackTrace(e.getCause()) : ExceptionUtils
                .getStackTrace(e);
        logError(e);
        return getModelAndView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(Exception e) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        logError(e);
        triggerCriticalAlert(e);
        return getModelAndView(e, stackTrace);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ModelAndView handleException(LoginException e) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        logError(e);
        triggerCriticalAlert(e);
        return getModelAndView(e, stackTrace);
    }

    private ModelAndView getModelAndView(Exception e, String stackTrace) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", emptyStringIfNull(e.getMessage()), "stackTrace", stackTrace));
    }

    private ModelAndView getModelAndView(LedpException e, String stackTrace) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", emptyStringIfNull(e.getMessage()), "stackTrace", stackTrace));
    }

    private String emptyStringIfNull(Object o) {
        return o != null ? o.toString() : "";
    }

}
