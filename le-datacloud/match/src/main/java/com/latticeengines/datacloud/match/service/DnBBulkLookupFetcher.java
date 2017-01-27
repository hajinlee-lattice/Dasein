package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;

public interface DnBBulkLookupFetcher {
    public DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext);
}
