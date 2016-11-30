package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;

public interface AnalyticPipelineEntityMgr extends BaseEntityMgr<AnalyticPipeline> {

    AnalyticPipeline findByName(String analyticPipelineName);

    AnalyticPipeline getLatestProductionVersion();
}
