package com.latticeengines.apps.lp.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineReository extends BaseJpaRepository<RatingEngine, Long> {

    RatingEngine findById(String engineId);

}
