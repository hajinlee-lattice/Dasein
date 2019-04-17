package com.latticeengines.playmaker.service;

import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

public interface PlaymakerRateLimitingService {

    RateLimitedAcquisition acquirePlaymakerRequest(String tenant, boolean attemptOnly);

}
