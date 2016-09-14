package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.modelquality.Sampling;

public interface SamplingService {

    Sampling createLatestProductionSamplingConfig();
}
