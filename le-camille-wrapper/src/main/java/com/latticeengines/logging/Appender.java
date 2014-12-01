package com.latticeengines.logging;

/**
 * Appenders implement logging callback methods. Appenders should not use
 * loggers themselves.
 */
public interface Appender {
    public void info(String logger, String message);

    public void debug(String logger, String message);

    public void trace(String logger, String message);

    public void warn(String logger, String message);

    public void error(String logger, String message);
}
