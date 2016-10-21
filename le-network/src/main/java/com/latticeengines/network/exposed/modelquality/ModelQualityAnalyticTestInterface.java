package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;

public interface ModelQualityAnalyticTestInterface {

    List<AnalyticTestEntityNames> getAnalyticTests();

    String createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames);

    AnalyticTestEntityNames getAnalyticTestByName(String analyticTestName);
    
    List<String> executeAnalyticTestByName(String analyticTestName);

}
