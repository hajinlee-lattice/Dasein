package com.latticeengines.scoringapi.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpException;

@ControllerAdvice
public class ScoringApiExceptionHandler {
    private static final Log log = LogFactory.getLog(ScoringApiExceptionHandler.class);

    public ScoringApiExceptionHandler() {
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleException(ScoringApiException ex) {
        return generateExceptionResponse(ex.getCode().getExternalCode(), ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(LedpException ex) {
        return generateExceptionResponse("api_error", ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleException(HttpMessageNotReadableException ex) {
        return generateExceptionResponse("message_not_readable", ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(HttpMessageNotWritableException ex) {
        return generateExceptionResponse("message_not_writable", ex);
    }

    @ExceptionHandler
    @ResponseBody
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleException(Exception ex) {
        return generateExceptionResponse("general_error", ex);
    }

    private Map<String, Object> generateExceptionResponse(String code, Exception ex) {
        return generateExceptionResponse(code, ex, false, false);
    }

    private Map<String, Object> generateExceptionResponse(String code, Exception ex, boolean includeErrors,
            boolean includeTrace) {
        List<String> errorMessages = new ArrayList<String>();
        Throwable cause = ex;
        while (cause != null) {
            errorMessages.add(cause.getMessage());
            cause = cause.getCause();
        }
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String trace = sw.toString();

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("error", code);
        details.put("error_description", ex.getMessage());

        log.error(JsonUtils.serialize(details) + "\n" + trace);

        if (includeErrors) {
            details.put("errors", errorMessages);
        }

        if (includeTrace) {
            details.put("trace", trace);
        }

        return details;
    }

}
