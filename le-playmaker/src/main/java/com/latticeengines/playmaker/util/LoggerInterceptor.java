package com.latticeengines.playmaker.util;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

public class LoggerInterceptor extends HandlerInterceptorAdapter {

    private static final Logger log = LoggerFactory.getLogger(LoggerInterceptor.class);

    private static final String START_TIME = "startTime";

    private static final List<String> loggingForUris = //
            Arrays.asList( //
                    "/playmaker/contactextensionschema", //
                    "/playmaker/plays");

    @Inject
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {
            String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
            long startTime = System.currentTimeMillis();
            log.info("Playmaker Request URL:" + request.getRequestURL().toString() + " Start Time=" + startTime
                    + " Tenant=" + tenantName + " Referrer=" + request.getHeader(HttpHeaders.REFERER));

            request.setAttribute(START_TIME, startTime);
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
            Exception ex) {
        if (loggingForUris.stream().anyMatch(str -> request.getRequestURI().matches(str))) {
            String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
            long startTime = (Long) request.getAttribute(START_TIME);
            log.info("Playmaker Request URL:" + request.getRequestURL().toString() + " Time Taken="
                    + (System.currentTimeMillis() - startTime) + "ms" + " Tenant=" + tenantName + " Referrer="
                    + request.getHeader(HttpHeaders.REFERER));
        }
    }
}
