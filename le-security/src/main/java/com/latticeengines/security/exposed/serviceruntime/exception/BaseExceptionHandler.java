package com.latticeengines.security.exposed.serviceruntime.exception;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public abstract class BaseExceptionHandler {

    @Inject
    protected AlertService alertService;

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

    void triggerCriticalAlert(Exception e) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        List<BasicNameValuePair> details = new ArrayList<>();
        details.add(new BasicNameValuePair("stackTrace", stackTrace));
        String tenant = "";
        if (MultiTenantContext.getTenant() != null) {
            tenant = MultiTenantContext.getTenant().getId();
            details.add(new BasicNameValuePair("tenant", tenant));
        }

        String dedupKey = getCurrentRequest().getRequestURL().toString() + "|" + e.getClass().getName() + "|" + tenant;
        String message = StringUtils.isBlank(e.getMessage()) ? e.toString() : e.getMessage();
        alertService.triggerCriticalEvent(message, null, dedupKey, details);
    }

    String emptyStringIfNull(Object o) {
        return o != null ? o.toString() : "";
    }

}
