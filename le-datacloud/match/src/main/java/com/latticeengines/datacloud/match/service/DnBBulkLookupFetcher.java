package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;

public interface DnBBulkLookupFetcher {
    public DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext);
}
