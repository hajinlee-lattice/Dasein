package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineRepository extends BaseJpaRepository<RatingEngine, Long> {

    public List<RatingEngine> findByDeletedTrue();

}
