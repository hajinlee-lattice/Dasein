package com.latticeengines.modelquality.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryMetrics;

public interface ModelSummaryMetricsEntityMgr extends BaseEntityMgr<ModelSummaryMetrics> {

    List<ModelSummaryMetrics> getAll();

    ModelSummaryMetrics getByModelId(String modelId);

    void updateLastUpdateTime(ModelSummaryMetrics summary);

}
