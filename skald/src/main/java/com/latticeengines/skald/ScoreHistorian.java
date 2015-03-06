package com.latticeengines.skald;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

@Service
public class ScoreHistorian extends HandlerInterceptorAdapter {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (matchesMethod(request)) {
            request.setAttribute(ENTRY_KEY, new ScoreHistoryEntry());
        }

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {
        if (matchesMethod(request)) {
            log.info("Writing score history");

            ScoreHistoryEntry entry = (ScoreHistoryEntry) request.getAttribute(ENTRY_KEY);

            String identifier = (String) request.getAttribute(SkaldInterceptor.IDENTIFIER_KEY);
            long start = (long) request.getAttribute(SkaldInterceptor.START_TIME_KEY);

            entry.requestID = identifier;
            entry.received = start;
            entry.duration = System.currentTimeMillis() - start;

            // TODO Actually write the history entry to the database.
        }
    }

    private boolean matchesMethod(HttpServletRequest request) {
        // TODO Replace this with a type-safe matching mechanism.
        return request.getServletPath().equals("/ScoreRecord");
    }

    public static final String ENTRY_KEY = "ScoreHistoryEntry";

    private static final Log log = LogFactory.getLog(ScoreHistorian.class);
}
