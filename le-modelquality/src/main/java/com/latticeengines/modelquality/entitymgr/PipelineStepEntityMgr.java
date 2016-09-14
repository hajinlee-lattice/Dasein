package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;

public interface PipelineStepEntityMgr extends BaseEntityMgr<PipelineStep> {

    PipelineStep findByName(String stepName);
}
