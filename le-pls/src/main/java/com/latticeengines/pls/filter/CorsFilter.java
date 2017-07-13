package com.latticeengines.pls.filter;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import com.latticeengines.common.exposed.util.PropertyUtils;

public class CorsFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(CorsFilter.class);

    private static boolean allowCors = false;
    private static boolean initialized = false;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        initializeFlag();
        if (allowCors) {
            // Enables cross-origin-resource-sharing
            response.addHeader("Access-Control-Allow-Origin", "*");

            if (request.getHeader("Access-Control-Request-Method") != null && "OPTIONS".equals(request.getMethod())) {
                // CORS "pre-flight" request
                log.info("Enabling CORS in response header for pre-flight request.");
                response.addHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
                response.addHeader("Access-Control-Allow-Headers", "Content-Type");
                response.addHeader("Access-Control-Max-Age", "60");// 1 min
            }
        }

        filterChain.doFilter(request, response);
    }

    private static void initializeFlag() {
        if (!initialized) {
            allowCors = Boolean.valueOf(PropertyUtils.getProperty("common.allow.cors"));
            log.info("allow cors: " + allowCors);
            initialized = true;
        }
    }

}
