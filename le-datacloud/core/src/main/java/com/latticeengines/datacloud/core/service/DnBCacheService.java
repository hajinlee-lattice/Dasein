package com.latticeengines.datacloud.core.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;

public interface DnBCacheService {

    DnBCacheEntityMgr getCacheMgr();

    DnBCache lookupCache(DnBMatchContext context);

    Map<String, DnBCache> batchLookupCache(Map<String, DnBMatchContext> contexts);

    DnBCache addCache(DnBMatchContext context, boolean sync);

    List<DnBCache> batchAddCache(List<DnBMatchContext> contexts);

    void removeCache(DnBCache cache);

    void dumpQueue();

    int getQueueSize();

}
