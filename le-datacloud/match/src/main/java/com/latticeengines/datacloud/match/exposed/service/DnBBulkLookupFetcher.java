package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;

import java.util.List;

public interface DnBBulkLookupFetcher {
    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
