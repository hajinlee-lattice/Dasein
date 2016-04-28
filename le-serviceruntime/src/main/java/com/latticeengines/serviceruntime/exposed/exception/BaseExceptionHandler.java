package com.latticeengines.serviceruntime.exposed.exception;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public abstract class BaseExceptionHandler {
    private static final Log log = LogFactory.getLog(BaseExceptionHandler.class);

    protected void logError(String message) {
        HttpServletRequest request = getCurrentRequest();
        log.error("Request for " + request.getRequestURL() + " failed:\n" + message);
    }

    protected HttpServletRequest getCurrentRequest() {
        return ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
    }
}
