package com.latticeengines.serviceruntime.exposed.exception;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public abstract class FrontEndFacingExceptionHandler extends BaseExceptionHandler {

    @Autowired
    private AlertService alertService;

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException e) {
        String stackTrace = e.getErrorDetails().getStackTrace();
        logError(e.getCode() + "\n" + stackTrace);
        return getModelAndView(e);
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(Exception e) {
        String stackTrace = ExceptionUtils.getFullStackTrace(e);
        logError(stackTrace);

        List<BasicNameValuePair> details = new ArrayList<>();
        details.add(new BasicNameValuePair("stackTrace", stackTrace));
        this.alertService.triggerCriticalEvent(e.getMessage(), null, details);

        return getModelAndView();
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
