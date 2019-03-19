package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

public interface BulkScoringRateLimitingService {

    RateLimitedAcquisition acquireBulkRequest(String tenant, long numRows, boolean attemptOnly);

}
