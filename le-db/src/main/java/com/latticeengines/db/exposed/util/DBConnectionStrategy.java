package com.latticeengines.db.exposed.util;

public interface DBConnectionStrategy {

    Boolean isReaderConnection();

    void setReaderConnection(Boolean readerConnection);
}
