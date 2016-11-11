package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.entitymgr.DnBWhiteCacheEntityMgr;

public interface DnBCacheLookupService {
    DnBWhiteCacheEntityMgr getWhiteCacheMgr(String version);

    DnBWhiteCache lookupWhiteCache(MatchKeyTuple matchKeyTuple, String dataCloudVersion);

    Map<String, DnBWhiteCache> batchLookupWhiteCache(Map<String, MatchKeyTuple> matchKeyTuples,
            String dataCloudVersion);

    DnBWhiteCache addWhiteCache(DnBMatchContext context, String dataCloudVersion);

    List<DnBWhiteCache> batchAddWhiteCache(List<DnBMatchContext> contexts, String dataCloudVersion);
}
