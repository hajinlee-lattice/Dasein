package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;

public interface DnBBulkLookupStatusChecker {

    List<DnBBatchMatchContext> checkStatus(List<DnBBatchMatchContext> batchContexts);

}
