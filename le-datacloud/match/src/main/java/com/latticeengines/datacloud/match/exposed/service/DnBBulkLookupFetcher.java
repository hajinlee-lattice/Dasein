package com.latticeengines.datacloud.match.exposed.service;

import java.util.Map;

import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;

public interface DnBBulkLookupFetcher {
    public Map<String, DnBMatchContext> getResult(DnBBulkMatchInfo info);
}
