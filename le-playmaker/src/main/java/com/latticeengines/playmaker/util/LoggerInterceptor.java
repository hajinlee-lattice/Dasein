package com.latticeengines.playmaker.util;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class LoggerInterceptor extends HandlerInterceptorAdapter {

    private static final Logger log = LoggerFactory.getLogger(LoggerInterceptor.class);

    private static final String START_TIME = "startTime";

    private static final List<String> loggingForUris = //
            Arrays.asList( //
                    "/playmaker/contactextensionschema", //
                    "/playmaker/plays");

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {

            long startTime = System.currentTimeMillis();
            log.info("Playmaker Request URL:" + request.getRequestURL().toString() + " Start Time=" + startTime);
            request.setAttribute(START_TIME, startTime);
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
            Exception ex) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {
            long startTime = (Long) request.getAttribute(START_TIME);

            log.info("Playmaker Request URL:" + request.getRequestURL().toString() + " Time Taken="
                    + (System.currentTimeMillis() - startTime) + "ms");
        }
    }
}
