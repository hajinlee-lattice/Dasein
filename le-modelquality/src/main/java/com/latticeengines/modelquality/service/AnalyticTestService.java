package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;

public interface AnalyticTestService {

    AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames);
}
