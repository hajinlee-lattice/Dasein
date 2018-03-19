package com.latticeengines.pls.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface BucketedScoreSummaryRepository extends BaseJpaRepository<BucketedScoreSummary, Long> {

    BucketedScoreSummary findByModelSummary(ModelSummary modelSummary);
}
