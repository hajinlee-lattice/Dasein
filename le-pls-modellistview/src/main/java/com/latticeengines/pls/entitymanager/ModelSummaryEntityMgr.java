package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryEntityMgr extends BaseEntityMgr<ModelSummary> {

    ModelSummary findByModelId(String modelId);
    
    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument);
    
    void deleteByModelId(String modelId);
    
    void updateModelSummary(ModelSummary modelSummary);

}
