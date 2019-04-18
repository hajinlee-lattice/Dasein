package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;

public interface DnBBulkLookupFetcher {

    DnBBatchMatchContext getResult(DnBBatchMatchContext batchContext);

}
