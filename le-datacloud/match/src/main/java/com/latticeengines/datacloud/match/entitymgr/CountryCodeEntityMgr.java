package com.latticeengines.datacloud.match.entitymgr;

import java.util.concurrent.ConcurrentMap;

public interface CountryCodeEntityMgr {
    ConcurrentMap<String, String> findAll();
}
