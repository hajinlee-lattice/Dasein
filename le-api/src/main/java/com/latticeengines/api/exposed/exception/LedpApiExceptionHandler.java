package com.latticeengines.api.exposed.exception;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJacksonJsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

@SuppressWarnings("deprecation")
@ControllerAdvice
public class LedpApiExceptionHandler {

    public LedpApiExceptionHandler() {
    }
    
    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(LedpException e) {
        MappingJacksonJsonView jsonView = new MappingJacksonJsonView();
        String stackTrace = e.getCause() != null ? ExceptionUtils.getFullStackTrace(e.getCause()) : "";
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", e.getCode().name(), //
                "errorMsg", e.getMessage() + "\n" + stackTrace));

    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ModelAndView handleException(Exception e) {
        MappingJacksonJsonView jsonView = new MappingJacksonJsonView();
        String stackTrace = ExceptionUtils.getFullStackTrace(e);
        return new ModelAndView(jsonView, ImmutableMap.of("errorCode", LedpCode.LEDP_00002.name(), //
                "errorMsg", stackTrace));
    }
}
