package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.entitymgr.DnBBlackCacheEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.DnBWhiteCacheEntityMgr;

public interface DnBCacheService {
    void addCache(DnBMatchContext context);

    // White Cache
    DnBWhiteCacheEntityMgr getWhiteCacheMgr();

    DnBWhiteCache lookupWhiteCache(DnBMatchContext context);

    Map<String, DnBWhiteCache> batchLookupWhiteCache(Map<String, DnBMatchContext> contexts);

    DnBWhiteCache addWhiteCache(DnBMatchContext context);

    List<DnBWhiteCache> batchAddWhiteCache(List<DnBMatchContext> contexts);

    void removeWhiteCache(DnBWhiteCache cache);

    // Black Cache
    DnBBlackCacheEntityMgr getBlackCacheMgr();

    DnBBlackCache lookupBlackCache(DnBMatchContext context);

    Map<String, DnBBlackCache> batchLookupBlackCache(Map<String, DnBMatchContext> contexts);

    DnBBlackCache addBlackCache(DnBMatchContext context);

    List<DnBBlackCache> batchAddBlackCache(List<DnBMatchContext> contexts);
}
