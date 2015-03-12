package com.latticeengines.skald;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.latticeengines.skald.BodyBufferFilter.BufferedServletRequest;
import com.latticeengines.skald.BodyBufferFilter.BufferedServletResponse;

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

            entry.request = IOUtils.toString(((BufferedServletRequest) request).getBody(), "UTF-8");
            entry.response = IOUtils.toString(((BufferedServletResponse) response).getBody(), "UTF-8");

            writeObject(properties.getHistoryConnection(), "SkaldScoreHistory", entry);
        }
    }

    private boolean matchesMethod(HttpServletRequest request) {
        // TODO Replace this with a type-safe matching mechanism.
        return request.getServletPath().equals("/ScoreRecord");
    }

    // TODO Find a library equivalent to this function or move it into some
    // generic utility class.
    private void writeObject(String address, String table, Object entry) {
        Field[] fields = entry.getClass().getFields();
        List<String> names = new ArrayList<String>();
        for (Field field : ScoreHistoryEntry.class.getFields()) {
            names.add(field.getName());
        }

        String columns = StringUtils.collectionToCommaDelimitedString(names);
        String values = StringUtils.collectionToCommaDelimitedString(Collections.nCopies(names.size(), "?"));
        String text = "INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ")";

        // TODO Add connection pooling.
        try (Connection connection = DriverManager.getConnection(address)) {
            PreparedStatement statement = connection.prepareStatement(text);
            for (int index = 0; index < fields.length; index++) {
                Object value = fields[index].get(entry);
                statement.setObject(index + 1, value);
            }

            statement.executeUpdate();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to write object to database table " + table + ": " + ex.getMessage(), ex);
        }
    }

    public static final String ENTRY_KEY = "ScoreHistoryEntry";

    @Autowired
    private SkaldProperties properties;

    private static final Log log = LogFactory.getLog(ScoreHistorian.class);
}
