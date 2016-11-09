package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;

public interface DnBBulkLookupFetcher {
    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
