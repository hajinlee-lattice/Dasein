package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

public interface RatingEngineRepository extends BaseJpaRepository<RatingEngine, Long> {

    List<RatingEngine> findByDeletedTrue();

    List<RatingEngine> findByType(RatingEngineType type);

    List<RatingEngine> findByStatus(RatingEngineStatus status);

    List<RatingEngine> findByTypeAndStatus(RatingEngineType type, RatingEngineStatus status);

}
