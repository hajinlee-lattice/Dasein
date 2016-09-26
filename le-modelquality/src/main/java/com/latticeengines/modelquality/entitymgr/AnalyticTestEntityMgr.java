package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;

public interface AnalyticTestEntityMgr extends BaseEntityMgr<AnalyticTest> {

    AnalyticTest findByName(String analyticTestName);

}
