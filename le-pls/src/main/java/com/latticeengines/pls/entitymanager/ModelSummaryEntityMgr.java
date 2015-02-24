package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;

public interface ModelSummaryEntityMgr extends BaseEntityMgr<ModelSummary> {

    ModelSummary getByModelId(String modelId);
    
    ModelSummary findValidByModelId(String modelId);
    
    ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    void deleteByModelId(String modelId);
    
    void updateModelSummary(ModelSummary modelSummary);

    List<ModelSummary> getAll();

    List<ModelSummary> findAllValid();

    void updateStatusByModelId(String modelId, ModelSummaryStatus status);


}
