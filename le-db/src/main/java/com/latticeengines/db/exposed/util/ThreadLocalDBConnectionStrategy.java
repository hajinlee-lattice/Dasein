package com.latticeengines.db.exposed.util;

public class ThreadLocalDBConnectionStrategy implements DBConnectionStrategy {

    private ThreadLocal<Boolean> isReaderConnection = new ThreadLocal<>();

    public ThreadLocalDBConnectionStrategy() {
        isReaderConnection.set(Boolean.FALSE);
    }

    @Override
    public Boolean isReaderConnection() {
        return isReaderConnection.get();
    }

    @Override
    public void setReaderConnection(Boolean readerConnection) {
        isReaderConnection.set(readerConnection);
    }
}
