package com.latticeengines.datacloud.core.service;

import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

public interface RateLimitingService {

    /**
     * @param numRows
     * @param attemptOnly:
     *            true: Only check whether there is quota available, don't
     *            really acquire quota
     * @return
     */
    RateLimitedAcquisition acquireDnBBulkRequest(long numRows, boolean attemptOnly);

    /**
     * @param attemptOnly:
     *            true: Only check whether there is quota available, don't
     *            really acquire quota
     * @return
     */
    RateLimitedAcquisition acquireDnBBulkStatus(boolean attemptOnly);

}
