package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Predictor;

public interface PredictorEntityMgr extends BaseEntityMgr<Predictor> {

    List<Predictor> findByModelId(String ModelId);

    List<Predictor> findUsedByBuyerInsightsViaModelId(String ModelId);

}
