package com.latticeengines.common.exposed.rest;

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;

@Component("requestLogInterceptor")
public class RequestLogInterceptor extends HandlerInterceptorAdapter {

    public static final String IDENTIFIER_KEY = "com.latticeengines.requestid";
    public static final String REQUEST_ID = "Request-Id";
    private static final String URI_KEY = "com.latticeengines.uri";

    private static final Logger log = LoggerFactory.getLogger(RequestLogInterceptor.class);

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {

        String identifier = getRequestId(request);
        request.setAttribute(IDENTIFIER_KEY, identifier);
        response.addHeader(REQUEST_ID, identifier);
        // It's safe to assume nothing has added to the MDC on this thread
        // already, so we can use it directly rather than through LogContext.
        MDC.put(IDENTIFIER_KEY, identifier);
        MDC.put(URI_KEY, request.getRequestURI());

        request.setAttribute(IDENTIFIER_KEY, identifier);
        httpStopWatch.start();

        String address = request.getHeader("X-FORWARDED-FOR");
        if (address == null) {
            address = request.getRemoteHost();
        }
        log.info(String.format("Received request from %s", address));

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {
        try {
            httpStopWatch.stop();
            long duration = httpStopWatch.getTime();
            log.info(String.format("{\"requestDurationMS\":\"%d\"}", duration));
        } finally {
            // If this doesn't get called, memory can permanently leak.
            MDC.remove(URI_KEY);
            MDC.remove(IDENTIFIER_KEY);
        }
    }

    protected String getRequestId(HttpServletRequest request) {
        String identifier = UUID.randomUUID().toString();
        return identifier;
    }

    public static String getRequestIdentifierId(HttpServletRequest request) {
        String requestId = "";
        Object identifier = request.getAttribute(RequestLogInterceptor.IDENTIFIER_KEY);
        if (!StringStandardizationUtils.objectIsNullOrEmptyString(identifier)) {
            requestId = String.valueOf(identifier);
        }
        return requestId;
    }
}
