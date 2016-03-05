package com.latticeengines.skald;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.common.exposed.rest.BodyBufferFilter.BufferedServletRequest;
import com.latticeengines.common.exposed.rest.BodyBufferFilter.BufferedServletResponse;
import com.latticeengines.common.exposed.rest.RequestLogInterceptor;

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

            String identifier = (String) request.getAttribute(RequestLogInterceptor.IDENTIFIER_KEY);
//            long start = (long) request.getAttribute(RequestLogInterceptor.START_TIME_KEY);

            entry.requestID = identifier;
//            entry.received = start;
//            entry.duration = System.currentTimeMillis() - start;

            entry.request = IOUtils.toString(((BufferedServletRequest) request).getBody(), "UTF-8");
            entry.response = IOUtils.toString(((BufferedServletResponse) response).getBody(), "UTF-8");

            // TODO Find a way to wrap this boilerplate in try-with-resources.
            Session session = hibernate.openSession();
            try {
                Transaction transaction = session.beginTransaction();
                session.save(entry);
                transaction.commit();
            } finally {
                session.close();
            }
        }
    }

    private boolean matchesMethod(HttpServletRequest request) {
        // TODO Replace this with a type-safe matching mechanism.
        return request.getServletPath().equals("/ScoreRecord");
    }

    public static final String ENTRY_KEY = "ScoreHistoryEntry";

    @Autowired
    private SkaldProperties properties;

    @Autowired
    private SkaldHibernate hibernate;

    private static final Log log = LogFactory.getLog(ScoreHistorian.class);
}
