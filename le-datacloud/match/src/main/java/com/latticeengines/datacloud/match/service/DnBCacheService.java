package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.match.dnb.DnBCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.entitymgr.DnBCacheEntityMgr;

public interface DnBCacheService {

    DnBCacheEntityMgr getCacheMgr();

    DnBCache lookupCache(DnBMatchContext context);

    Map<String, DnBCache> batchLookupCache(Map<String, DnBMatchContext> contexts);

    DnBCache addCache(DnBMatchContext context);

    List<DnBCache> batchAddCache(List<DnBMatchContext> contexts);

    void removeCache(DnBCache cache);

}
