package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;

public interface DnBBulkLookupDispatcher {
    public DnBBatchMatchContext sendRequest(DnBBatchMatchContext bulkMatchContext);
}
