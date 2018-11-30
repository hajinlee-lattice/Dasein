package com.latticeengines.scoringapi.exposed;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.common.exposed.rest.HttpStopWatch;

public class RequestLogInterceptor extends HandlerInterceptorAdapter {

    private static final String IDENTIFIER_KEY = "com.latticeengines.requestid";
    private static final String REQUEST_ID = "Request-Id";
    private static final String URI_KEY = "com.latticeengines.uri";

    private static final Logger log = LoggerFactory.getLogger(RequestLogInterceptor.class);

    private static final List<String> skipLoggingForUris = //
            Arrays.asList( //
                    "score/record", //
                    "scoreinternal/record");

    @Inject
    private HttpStopWatch httpStopWatch;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {

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
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, //
                                Exception ex) {
        try {
            httpStopWatch.stop();
            long duration = httpStopWatch.getTime();
            if (!shouldSkipLogging(request.getRequestURI())) {
                log.info(String.format("{\"requestDurationMS\":\"%d\"}", duration));
            }
        } finally {
            // If this doesn't get called, memory can permanently leak.
            MDC.remove(URI_KEY);
            MDC.remove(IDENTIFIER_KEY);
        }
    }

    private String getRequestId(HttpServletRequest request) {
        String identifier = request.getHeader(REQUEST_ID);
        if (StringUtils.isBlank(identifier)) {
            identifier = UUID.randomUUID().toString();
        }
        return identifier;
    }

    private boolean shouldSkipLogging(String requestURI) {
        Optional<String> result = skipLoggingForUris.stream() //
                .filter(requestURI::contains) //
                .findAny();
        return result.isPresent();
    }
}
