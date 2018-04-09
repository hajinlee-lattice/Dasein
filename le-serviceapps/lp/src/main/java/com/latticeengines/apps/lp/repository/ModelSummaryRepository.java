package com.latticeengines.apps.lp.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public interface ModelSummaryRepository extends BaseJpaRepository<ModelSummary, Long> {

    ModelSummary findById(String modelGuid);

}
