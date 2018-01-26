package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface PlayEntityMgr extends BaseEntityMgrRepository<Play, Long> {

    Play createOrUpdatePlay(Play entity);

    List<Play> findAll();

    List<Play> findAllByRatingEnginePid(long pid);

    List<Play> findByRatingEngineAndPlayStatusIn(RatingEngine ratingEngine, List<PlayStatus> statusList);

    Play findByName(String name);

    void deleteByName(String name);

    void createOrUpdate(Play play);

}
