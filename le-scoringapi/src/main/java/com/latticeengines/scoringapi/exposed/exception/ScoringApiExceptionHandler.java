package com.latticeengines.scoringapi.exposed.exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.ratelimit.RateLimitException;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;

@ControllerAdvice
// define precedence to avoid conflit with PropDataExceptionHandler
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ScoringApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ScoringApiExceptionHandler.class);

    @Inject
    private RequestInfo requestInfo;

    @Inject
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
        ScoringApiException exception = new ScoringApiException(LedpCode.LEDP_31111,
                new String[] { "message_not_readable" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public ExceptionHandlerErrors handleException(RateLimitException ex) {
        ScoringApiException exception = new ScoringApiException(LedpCode.LEDP_31111,
                new String[] { "too_many_requests" }, ex.getMessage());
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
        ScoringApiException exception = new ScoringApiException(LedpCode.LEDP_31111, new String[] { "general_error" },
                ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(HttpMessageNotWritableException ex) {
        ScoringApiException exception = new ScoringApiException(LedpCode.LEDP_31111,
                new String[] { "message_not_writable" }, ex.getMessage());
        exception.setStackTrace(ex.getStackTrace());
        return handleScoringApiException(exception);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ExceptionHandlerErrors handleException(IOException ex) {
        String trace = ExceptionUtils.getStackTrace(ex);
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
            trace = ExceptionUtils.getStackTrace(ex);
        }

        String exceptionHandlerErrorsMsg = JsonUtils.serialize(exceptionHandlerErrors);
        String errorMsg;
        if (ex instanceof ScoringApiException) {
            errorMsg = exceptionHandlerErrorsMsg + "\n" + ((ScoringApiException) ex).getDetailedMessage() + trace;
        } else {
            errorMsg = exceptionHandlerErrorsMsg + "\n" + trace;
        }
        log.error(errorMsg);

        requestInfo.put("HasWarning", String.valueOf(warnings.hasWarnings()));
        requestInfo.put("HasError", Boolean.toString(true));
        requestInfo.put("Error", exceptionHandlerErrorsMsg);
        if (warnings.hasWarnings()) {
            requestInfo.put("Warnings", JsonUtils.serialize(warnings.getWarnings()));
        }
        requestInfo.logSummary(requestInfo.getStopWatchSplits());

        return exceptionHandlerErrors;
    }
}
