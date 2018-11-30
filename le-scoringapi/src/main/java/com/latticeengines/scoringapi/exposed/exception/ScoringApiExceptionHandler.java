package com.latticeengines.scoringapi.exposed.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
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
import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.monitor.exposed.ratelimit.RateLimitException;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;

@ControllerAdvice
// define precedence to avoid conflit with PropDataExceptionHandler
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ScoringApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ScoringApiExceptionHandler.class);
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
            return handleScoringApiException((ScoringApiException) cause);
        } else if (cause instanceof LedpException) {
            return handleLedpException((LedpException) cause);
        } else {
            return generateExceptionResponse("general_error", ex, true);
        }
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ExceptionHandlerErrors handleException(HttpMessageNotReadableException ex) {
        ScoringApiException exception = new ScoringApiException(
                LedpCode.LEDP_31111,  new String[] { "message_not_readable" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public ExceptionHandlerErrors handleException(RateLimitException ex) {
        ScoringApiException exception = new ScoringApiException(
                LedpCode.LEDP_31111, new String[] { "too_many_requests" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(LedpException ex) {
        return handleLedpException(ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(Exception ex) {
        ScoringApiException exception = new ScoringApiException(
                LedpCode.LEDP_31111, new String[] { "general_error" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(HttpMessageNotWritableException ex) {
        ScoringApiException exception = new ScoringApiException(
                LedpCode.LEDP_31111, new String[] { "message_not_writable" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(IOException ex) {
        String trace = getStackTraceAsString(ex);
        if (trace.contains("org.apache.catalina.connector.ClientAbortException")) {
            return generateExceptionResponseNoAlert("client_abort", ex);
        } else {
            return generateExceptionResponse("io_error", ex, true);
        }
    }

    private ExceptionHandlerErrors handleScoringApiException(ScoringApiException ex) {
        return generateExceptionResponse(ex.getCode().getExternalCode(), ex, false, false, false);
    }

    private ExceptionHandlerErrors handleLedpException(LedpException ex) {
        return generateExceptionResponse("api_error", ex, true);
    }

    private ExceptionHandlerErrors generateExceptionResponseNoAlert(String code, Exception ex) {
        return generateExceptionResponse(code, ex, false);
    }

    private ExceptionHandlerErrors generateExceptionResponse(String code, Exception ex, boolean fireAlert) {
        return generateExceptionResponse(code, ex, fireAlert, false, true);
    }

    private ExceptionHandlerErrors generateExceptionResponse(String code, Exception ex, boolean fireAlert,
            boolean includeErrors, boolean includeTrace) {
        ExceptionHandlerErrors exceptionHandlerErrors = new ExceptionHandlerErrors();
        List<String> errorMessages = new ArrayList<>();
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
            trace = getStackTraceAsString(ex);
        }

        String exceptionHandlerErrorsMsg = JsonUtils.serialize(exceptionHandlerErrors);
        String errorMsg;
        if (ex instanceof ScoringApiException) {
            errorMsg = exceptionHandlerErrorsMsg + "\n" + ((ScoringApiException) ex).getDetailedMessage() + trace;
        } else {
            errorMsg = exceptionHandlerErrorsMsg + "\n" + trace;
        }
        log.error(errorMsg);

        if (fireAlert) {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder
                    .getRequestAttributes();
            String identifier = String
                    .valueOf(attributes.getRequest().getAttribute(RequestIdUtils.IDENTIFIER_KEY));

            List<BasicNameValuePair> alertDetails = new ArrayList<>();
            alertDetails.add(new BasicNameValuePair(RequestIdUtils.REQUEST_ID, identifier));
            alertDetails.add(new BasicNameValuePair("Tenant", requestInfo.get(RequestInfo.TENANT)));
            alertDetails.add(new BasicNameValuePair("Error Message:", errorMsg));

            String logUrl = SPLUNK_URL + identifier + "%22";
            String dedupKey = getClass().getName() + requestInfo.get(RequestInfo.TENANT) + "-" + code + "-"
                    + ex.getClass().getName();
            alertService.triggerCriticalEvent(errorMessage, logUrl, dedupKey, alertDetails);
        }

        requestInfo.put("HasWarning", String.valueOf(warnings.hasWarnings()));
        requestInfo.put("HasError", Boolean.toString(true));
        requestInfo.put("Error", exceptionHandlerErrorsMsg);
        if (warnings.hasWarnings()) {
            requestInfo.put("Warnings", JsonUtils.serialize(warnings.getWarnings()));
        }
        requestInfo.logSummary(requestInfo.getStopWatchSplits());

        return exceptionHandlerErrors;
    }

    private String getStackTraceAsString(Exception ex) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
