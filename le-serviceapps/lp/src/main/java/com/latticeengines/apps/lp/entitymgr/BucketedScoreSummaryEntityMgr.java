package com.latticeengines.apps.lp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface BucketedScoreSummaryEntityMgr extends BaseEntityMgrRepository<BucketedScoreSummary, Long> {

    BucketedScoreSummary findByModelGuid(String modelGuid);

    BucketedScoreSummary findByModelGuidFromReader(String modelGuid);
}
