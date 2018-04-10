package com.latticeengines.apps.lp.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public interface BucketedScoreSummaryEntityMgr extends BaseEntityMgrRepository<BucketedScoreSummary, Long> {

    BucketedScoreSummary getByModelGuid(String modelGuid);

    BucketedScoreSummary getByModelGuidFromReader(String modelGuid);
}
