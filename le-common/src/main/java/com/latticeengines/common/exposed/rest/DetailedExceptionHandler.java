package com.latticeengines.common.exposed.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

// TODO Find a more descriptive name for this class.
@ControllerAdvice(annotations = { DetailedErrors.class })
public class DetailedExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public Map<String, Object> handleException(Exception ex) {
        String trace = ExceptionUtils.getFullStackTrace(ex);
        log.error(trace);

        List<String> messages = new ArrayList<String>();
        Throwable cause = ex;
        while (cause != null) {
            String message = cause.getMessage();
            if (message == null) {
                message = cause.getClass().getSimpleName();
            }

            messages.add(cause.getMessage());
            cause = cause.getCause();
        }

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("summary", ex.getMessage());
        details.put("errors", messages);
        details.put("trace", trace);

        return details;
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers, HttpStatus status, WebRequest request) {
        log.error(ex);
        return handleExceptionInternal(ex, handleException(ex), headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotWritable(HttpMessageNotWritableException ex,
            HttpHeaders headers, HttpStatus status, WebRequest request) {
        log.error(ex);
        return handleExceptionInternal(ex, handleException(ex), headers, status, request);
    }

    private static final Log log = LogFactory.getLog(DetailedExceptionHandler.class);
}
