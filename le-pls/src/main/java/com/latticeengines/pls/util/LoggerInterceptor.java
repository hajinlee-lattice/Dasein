package com.latticeengines.pls.util;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class LoggerInterceptor extends HandlerInterceptorAdapter {

    private static final Logger log = LoggerFactory.getLogger(LoggerInterceptor.class);

    private static final String START_TIME = "startTime";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {

        long startTime = System.currentTimeMillis();
        log.info("PLS Request URL::" + request.getRequestURL().toString() + ":: Start Time=" + startTime);
        request.setAttribute(START_TIME, startTime);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
            Exception ex) {

        long startTime = (Long) request.getAttribute(START_TIME);

        log.info("PLS Request URL::" + request.getRequestURL().toString() + ":: Time Taken="
                + (System.currentTimeMillis() - startTime));
    }
}
