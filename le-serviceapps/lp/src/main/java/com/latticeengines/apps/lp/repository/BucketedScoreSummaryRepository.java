package com.latticeengines.apps.lp.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public interface BucketedScoreSummaryRepository extends BaseJpaRepository<BucketedScoreSummary, Long> {

    BucketedScoreSummary findByModelSummary_Id(String modelGuid);

}
