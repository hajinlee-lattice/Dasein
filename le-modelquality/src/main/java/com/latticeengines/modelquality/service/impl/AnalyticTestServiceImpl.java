package com.latticeengines.modelquality.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.modelquality.service.AnalyticTestService;

@Component("analyticTestService")
public class AnalyticTestServiceImpl extends BaseServiceImpl implements AnalyticTestService {

    @Override
    public AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames) {
        AnalyticTest analyticTest = new AnalyticTest();
        // NOTE: THIS IS A DUMMY IMPLEMENTATION THAT NEEDS TO BE COMPLETED
        return analyticTest;

    }
}
