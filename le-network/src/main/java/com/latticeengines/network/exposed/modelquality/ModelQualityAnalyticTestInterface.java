package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.ModelRun;

public interface ModelQualityAnalyticTestInterface {

    List<AnalyticTestEntityNames> getAnalyticTests();

    String createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames);

    AnalyticTestEntityNames getAnalyticTestByName(String analyticTestName);

    List<ModelRun> executeAnalyticTestByName(String analyticTestName);

    List<AnalyticTest> updateProductionAnalyticPipeline();
}
