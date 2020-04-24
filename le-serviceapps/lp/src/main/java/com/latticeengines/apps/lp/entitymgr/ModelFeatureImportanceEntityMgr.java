package com.latticeengines.apps.lp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

public interface ModelFeatureImportanceEntityMgr extends BaseEntityMgrRepository<ModelFeatureImportance, Long> {

    void createFeatureImportances(List<ModelFeatureImportance> importances);

    List<ModelFeatureImportance> getByModelGuid(String modelGuid);

}
