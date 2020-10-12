package com.latticeengines.security.exposed.serviceruntime.exception;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public abstract class BaseExceptionHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    void logWarning(String message) {
        HttpServletRequest request = getCurrentRequest();
        log.warn("Request for " + request.getRequestURL() + " failed:\n" + message);
    }

    protected void logError(String message) {
        HttpServletRequest request = getCurrentRequest();
        log.error("Request for " + request.getRequestURL() + " failed:\n" + message);
    }

    protected void logError(Throwable t) {
        HttpServletRequest request = getCurrentRequest();
        log.error("Request for " + request.getRequestURL() + " failed", t);
    }

    protected HttpServletRequest getCurrentRequest() {
        return ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
    }

    String emptyStringIfNull(Object o) {
        return o != null ? o.toString() : "";
    }

}
