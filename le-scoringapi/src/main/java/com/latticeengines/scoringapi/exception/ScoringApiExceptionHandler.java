package com.latticeengines.scoringapi.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

@ControllerAdvice
public class ScoringApiExceptionHandler {
    private static final Log log = LogFactory.getLog(ScoringApiExceptionHandler.class);

    @Autowired
    private AlertService alertService;

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleException(ScoringApiException ex) {
        return handleScoringApiException(ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(UncheckedExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof ScoringApiException) {
            ScoringApiException scoringEx = (ScoringApiException) cause;
            return handleScoringApiException(scoringEx);
        } else if (cause instanceof LedpException) {
            LedpException ledpEx = (LedpException) cause;
            return handleLedpException(ledpEx);
        } else {
            return generateExceptionResponse("general_error", ex, true);
        }
    }

    private Map<String, Object> handleScoringApiException(ScoringApiException ex) {
        return generateExceptionResponse(ex.getCode().getExternalCode(), ex, false, false, false);
    }

    private Map<String, Object> handleLedpException(LedpException ex) {
        return generateExceptionResponse("api_error", ex, true);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(LedpException ex) {
        return handleLedpException(ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleException(HttpMessageNotReadableException ex) {
        return generateExceptionResponse("message_not_readable", ex, false);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(HttpMessageNotWritableException ex) {
        return generateExceptionResponse("message_not_writable", ex, false);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(Exception ex) {
        return generateExceptionResponse("general_error", ex, true);
    }

    private Map<String, Object> generateExceptionResponse(String code, Exception ex, boolean fireAlert) {
        return generateExceptionResponse(code, ex, fireAlert, false, true);
    }

    private Map<String, Object> generateExceptionResponse(String code, Exception ex, boolean fireAlert,
            boolean includeErrors, boolean includeTrace) {
        List<String> errorMessages = new ArrayList<String>();
        Throwable cause = ex;
        while (cause != null) {
            errorMessages.add(cause.getMessage());
            cause = cause.getCause();
        }

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("error", code);
        details.put("error_description", ex.getMessage());



        if (includeErrors) {
            details.put("errors", errorMessages);
        }

        String trace = "";
        if (includeTrace) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            trace = sw.toString();
        }

        log.error(JsonUtils.serialize(details) + "\n" + trace);

        return details;
    }

}
