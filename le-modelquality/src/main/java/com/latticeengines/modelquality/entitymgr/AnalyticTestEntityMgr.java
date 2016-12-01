package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;

public interface AnalyticTestEntityMgr extends BaseEntityMgr<AnalyticTest> {

    AnalyticTest findByName(String analyticTestName);

    List<AnalyticTest> findAllByAnalyticPipeline(AnalyticPipeline ap);
}
