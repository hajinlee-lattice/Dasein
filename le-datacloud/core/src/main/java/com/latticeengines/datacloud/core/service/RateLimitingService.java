package com.latticeengines.datacloud.core.service;

import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

public interface RateLimitingService {

    RateLimitedAcquisition acquireDnBBulkRequest(long numRows, boolean withoutAcquireQuota);

    RateLimitedAcquisition acquireDnBBulkStatus();

}
