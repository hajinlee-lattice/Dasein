package com.latticeengines.apps.lp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryEntityMgr extends BaseEntityMgrRepository<ModelSummary, Long> {

    void updateLastUpdateTime(String modelGuid);

}
