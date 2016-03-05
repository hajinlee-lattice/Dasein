package com.latticeengines.common.exposed.rest;

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

@Component("requestLogInterceptor")
public class RequestLogInterceptor extends HandlerInterceptorAdapter {

    private static final String URI_KEY = "com.latticeengines.uri";
    public static final String IDENTIFIER_KEY = "com.latticeengines.requestid";

    private static final Log log = LogFactory.getLog(RequestLogInterceptor.class);

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        String identifier = UUID.randomUUID().toString();
        request.setAttribute(IDENTIFIER_KEY, identifier);
        response.addHeader("Request-Id", identifier);
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
            log.info(String.format("Request took %dms", duration));
        } finally {
            // If this doesn't get called, memory can permanently leak.
            MDC.remove(URI_KEY);
            MDC.remove(IDENTIFIER_KEY);
        }
    }

}
