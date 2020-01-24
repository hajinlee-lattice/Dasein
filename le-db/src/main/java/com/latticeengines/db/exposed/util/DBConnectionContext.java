package com.latticeengines.db.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DBConnectionContext {

    protected DBConnectionContext() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(DBConnectionContext.class);

    private static DBConnectionStrategy strategy = new ThreadLocalDBConnectionStrategy();

    public static Boolean isReaderConnection() {
        return strategy.isReaderConnection();
    }

    public static void setReaderConnection(Boolean readerConnection) {
        log.info("Set use reader connection to: " + String.valueOf(readerConnection));
        strategy.setReaderConnection(readerConnection);
    }
}
