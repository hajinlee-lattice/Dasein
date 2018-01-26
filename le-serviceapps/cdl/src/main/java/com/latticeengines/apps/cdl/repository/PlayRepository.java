package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface PlayRepository extends BaseJpaRepository<Play, Long> {

    public List<Play> findByRatingEngineAndPlayStatusIn(RatingEngine ratingEngine, List<PlayStatus> statusList);

}
