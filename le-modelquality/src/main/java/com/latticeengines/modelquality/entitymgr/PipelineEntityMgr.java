package com.latticeengines.modelquality.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.modelquality.Pipeline;

public interface PipelineEntityMgr extends BaseEntityMgr<Pipeline> {

    Pipeline findByName(String name);
    
    Pipeline getLatestProductionVersion();
}
