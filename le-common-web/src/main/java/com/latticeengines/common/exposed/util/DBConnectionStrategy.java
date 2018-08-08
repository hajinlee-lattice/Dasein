package com.latticeengines.common.exposed.util;

public interface DBConnectionStrategy {

    Boolean isReaderConnection();

    void setReaderConnection(Boolean readerConnection);
}
