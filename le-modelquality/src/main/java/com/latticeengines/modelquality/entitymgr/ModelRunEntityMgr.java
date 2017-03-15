package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.ModelRun;

public interface ModelRunEntityMgr extends BaseEntityMgr<ModelRun> {

    ModelRun findByName(String modelRunName);

    List<ModelRun> findModelRunsByAnalyticTest(AnalyticTest analyticTestName);
}
