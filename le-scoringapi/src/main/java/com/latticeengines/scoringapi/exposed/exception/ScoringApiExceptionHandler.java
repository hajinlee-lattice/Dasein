package com.latticeengines.scoringapi.exposed.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;

@ControllerAdvice
public class ScoringApiExceptionHandler {

    private static final Log log = LogFactory.getLog(ScoringApiExceptionHandler.class);
    private static final String SPLUNK_URL = "http://splunk.lattice.local:8000/en-US/app/search/search?q=search%20index%3Dscoringapi%20%22";

    @Autowired
    private AlertService alertService;

    @Autowired
    private RequestInfo requestInfo;

    @Autowired
    private Warnings warnings;

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionHandlerErrors handleException(ScoringApiException ex) {
        return handleScoringApiException(ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionHandlerErrors handleException(UncheckedExecutionException ex) {
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

    private ExceptionHandlerErrors handleScoringApiException(ScoringApiException ex) {
        return generateExceptionResponse(ex.getCode().getExternalCode(), ex, false, false, false);
    }

    private ExceptionHandlerErrors handleLedpException(LedpException ex) {
        return generateExceptionResponse("api_error", ex, true);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(LedpException ex) {
        return handleLedpException(ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionHandlerErrors handleException(HttpMessageNotReadableException ex) {
        return generateExceptionResponse("message_not_readable", ex, false);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(HttpMessageNotWritableException ex) {
        return generateExceptionResponse("message_not_writable", ex, false);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(Exception ex) {
        return generateExceptionResponse("general_error", ex, true);
    }

    private ExceptionHandlerErrors generateExceptionResponse(String code, Exception ex, boolean fireAlert) {
        return generateExceptionResponse(code, ex, fireAlert, false, true);
    }

    private ExceptionHandlerErrors generateExceptionResponse(String code, Exception ex, boolean fireAlert,
            boolean includeErrors, boolean includeTrace) {
        ExceptionHandlerErrors exceptionHandlerErrors = new ExceptionHandlerErrors();
        List<String> errorMessages = new ArrayList<String>();
        Throwable cause = ex;
        while (cause != null) {
            errorMessages.add(cause.getMessage());
            cause = cause.getCause();
        }

        String errorMessage = ex.getMessage();
        exceptionHandlerErrors.setError(code);
        exceptionHandlerErrors.setDescription(errorMessage);

        if (includeErrors) {
            exceptionHandlerErrors.setErrors(errorMessages);
        }

        String trace = "";
        if (includeTrace) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            trace = sw.toString();
        }

        String errorMsg = JsonUtils.serialize(exceptionHandlerErrors) + "\n" + trace;
        log.error(errorMsg);

        if (fireAlert) {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            String identifier = String.valueOf(attributes.getRequest().getAttribute(RequestLogInterceptor.IDENTIFIER_KEY));

            List<BasicNameValuePair> alertDetails = new ArrayList<>();
            alertDetails.add(new BasicNameValuePair(RequestLogInterceptor.REQUEST_ID, identifier));
            alertDetails.add(new BasicNameValuePair("Error Message:", errorMsg));

            String logUrl = SPLUNK_URL + identifier + "%22";
            alertService.triggerCriticalEvent(errorMessage, logUrl, alertDetails);
        }

        requestInfo.put("HasWarning", String.valueOf(!warnings.getWarnings().isEmpty()));
        requestInfo.put("HasError", Boolean.toString(true));
        requestInfo.logSummary();

        return exceptionHandlerErrors;
    }
}
