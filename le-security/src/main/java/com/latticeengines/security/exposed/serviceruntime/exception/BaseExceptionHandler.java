package com.latticeengines.security.exposed.serviceruntime.exception;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public abstract class BaseExceptionHandler {
    @Autowired
    private AlertService alertService;

    private static final Log log = LogFactory.getLog(BaseExceptionHandler.class);

    protected void logError(String message) {
        HttpServletRequest request = getCurrentRequest();
        log.error("Request for " + request.getRequestURL() + " failed:\n" + message);
    }

    protected HttpServletRequest getCurrentRequest() {
        return ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
    }

    protected void triggerCriticalAlert(Exception e) {
        String stackTrace = ExceptionUtils.getFullStackTrace(e);
        List<BasicNameValuePair> details = new ArrayList<>();
        details.add(new BasicNameValuePair("stackTrace", stackTrace));
        String tenant = "";
        if (MultiTenantContext.getTenant() != null) {
            tenant = MultiTenantContext.getTenant().getId();
            details.add(new BasicNameValuePair("tenant", tenant));
        }

        String dedupKey = getCurrentRequest().getRequestURL().toString() + "|" + e.getClass().getName() + "|" + tenant;
        this.alertService.triggerCriticalEvent(e.getMessage(), null, dedupKey, details);
    }
}
