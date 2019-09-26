package com.latticeengines.pls.util;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;

public class LoggerInterceptor extends HandlerInterceptorAdapter {

    private static final Logger log = LoggerFactory.getLogger(LoggerInterceptor.class);

    private static final String START_TIME = "startTime";

    private static final List<String> loggingForUris = //
            Arrays.asList( //
                    "/pls/entities/counts", //
                    "/pls/accounts/count", //
                    "/pls/accounts/data", //
                    "/pls/contacts/count", //
                    "/pls/contacts/data", //
                    "/pls/products/data", //
                    "/pls/datacollection/segment", //
                    "/pls/datacollection/segments/export", //
                    "/pls/ratingengines/(.*)/entitypreview/count", //
                    "/pls/ratingengines/coverage", //
                    "/pls/ratingengines/coverage/segment/(.*)", //
                    "/pls/dante/attributes", //
                    "/pls/dante/attributes/accountattributes", //
                    "/pls/play/(.*)/launches/(.*)/launch", //
                    "/pls/play/(.*)/launches/(.*)/kickoff-launch");

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {

            long startTime = System.currentTimeMillis();
            Tenant tenant = MultiTenantContext.getTenant();
            log.info("PLS Request URL:" + request.getRequestURL().toString() + " Start Time=" + startTime + " Tenant="
                    + tenant.getName());
            request.setAttribute(START_TIME, startTime);
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
            Exception ex) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {
            long startTime = (Long) request.getAttribute(START_TIME);
            Tenant tenant = MultiTenantContext.getTenant();
            log.info("PLS Request URL:" + request.getRequestURL().toString() + " Time Taken="
                    + (System.currentTimeMillis() - startTime) + "ms" + " Tenant=" + tenant.getName());
        }
    }
}
