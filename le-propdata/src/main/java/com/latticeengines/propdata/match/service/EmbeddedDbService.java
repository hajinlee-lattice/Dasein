package com.latticeengines.propdata.match.service;

import java.util.concurrent.locks.Lock;

public interface EmbeddedDbService {

    void loadAsync();

    Boolean isReady();

    void invalidate();

    Lock readLock();

}
