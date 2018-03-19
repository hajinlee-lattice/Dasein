package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface BucketedScoreSummaryEntityMgr extends BaseEntityMgrRepository<BucketedScoreSummary, Long> {

    BucketedScoreSummary findByModelSummary(ModelSummary modelSummary);
}
