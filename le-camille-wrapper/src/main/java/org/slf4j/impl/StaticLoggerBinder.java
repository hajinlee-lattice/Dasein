package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import com.latticeengines.logging.LoggerAdapter;

public final class StaticLoggerBinder implements LoggerFactoryBinder {
    /**
     * Declare the version of the SLF4J API this implementation is
     * compiled against. The value of this field is usually modified
     * with each release.
     */
    public static final String REQUESTED_API_VERSION = "1.6.6";

    private static static finalLoggerBinder singleton = new StaticLoggerBinder();

    private final ILoggerFactory loggerFactory = LoggerAdapter.newLoggerFactory();

    public static static finalLoggerBinder getSingleton() {
    	return singleton;
    }

    private StaticLoggerBinder() { }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public String getLoggerFactoryClassStr() {
        return loggerFactory.getClass().getName();
    }
}
