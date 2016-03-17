package com.latticeengines.metadata.exception;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.security.exposed.exception.SecurityControllerExceptionHandler;

@ControllerAdvice
public class MetadataControllerExceptionHandler extends SecurityControllerExceptionHandler {
    private static final Logger log = Logger.getLogger(MetadataControllerExceptionHandler.class);

    @Autowired
    private AlertService alertService;

    public MetadataControllerExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = e.getCause() != null ? //
        ExceptionUtils.getFullStackTrace(e.getCause())
                : //
                ExceptionUtils.getStackTrace(e);
        log.error(e.getCode() + "\n" + stackTrace);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage()));
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(Exception e) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        String stackTrace = ExceptionUtils.getFullStackTrace(e);
        log.error(stackTrace);

        List<BasicNameValuePair> details = new ArrayList<>();
        details.add(new BasicNameValuePair("stackTrace", stackTrace));
        this.alertService.triggerCriticalEvent(e.getMessage(), null, details);

        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", stackTrace));
    }
}
