package com.latticeengines.modelquality.service;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;

public interface AnalyticTestService {

    AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames);
    
    AnalyticTestEntityNames getByName(String name);
    
    List<AnalyticTestEntityNames> getAll();
    
    List<String> executeByName(String name);
}
