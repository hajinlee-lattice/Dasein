package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.MDC;

// Wraps MDC entries in a structure that fits into a try with resources statement.
// This guarantees removal of entries from the MDC, which prevents memory leaks.
// Currently handles duplicate keys by keeping the first entry, but that will
// probably be replaced with better behavior later.
public class LogContext implements AutoCloseable {
    public LogContext(String key, Object value) {
        add(key, value);
    }

    public LogContext(Map<String, Object> entries) {
        for (Map.Entry<String, Object> entry : entries.entrySet()) {
            add(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void close() {
        for (String key : keys) {
            MDC.remove(key);
        }
    }

    private void add(String key, Object value) {
        if (MDC.get(key) == null) {
            MDC.put(key, value);
            keys.add(key);
        }
    }

    private final List<String> keys = new ArrayList<String>();
}
