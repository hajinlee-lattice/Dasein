package com.latticeengines.skald;

import java.security.SecureRandom;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

@Service
public class SkaldInterceptor extends HandlerInterceptorAdapter {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // TODO Generate a better request identifier.
        String identifier = Long.toString(Math.abs(generator.nextLong()), Character.MAX_RADIX);
        request.setAttribute(IDENTIFIER_KEY, identifier);

        // It's safe to assume nothing has added to the MDC on this thread
        // already, so we can use it directly rather than through LogContext.
        MDC.put(IDENTIFIER_KEY, identifier);
        MDC.put(PATH_KEY, request.getServletPath().substring(1));

        request.setAttribute(IDENTIFIER_KEY, identifier);
        request.setAttribute(START_TIME_KEY, System.currentTimeMillis());

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
            long start = (long) request.getAttribute(START_TIME_KEY);
            long duration = System.currentTimeMillis() - start;
            log.info(String.format("Request took %dms", duration));
        } finally {
            // If this doesn't get called, memory can permanently leak.
            MDC.remove(PATH_KEY);
            MDC.remove(IDENTIFIER_KEY);
        }
    }

    public static final String PATH_KEY = "Path";
    public static final String IDENTIFIER_KEY = "Identifier";
    public static final String START_TIME_KEY = "Started";

    private static final SecureRandom generator = new SecureRandom();

    private static final Log log = LogFactory.getLog(SkaldInterceptor.class);
}
