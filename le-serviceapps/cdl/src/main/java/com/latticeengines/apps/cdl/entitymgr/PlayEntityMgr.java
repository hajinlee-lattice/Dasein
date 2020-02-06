package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface PlayEntityMgr extends BaseEntityMgrRepository<Play, Long> {

    List<Play> findAll();

    List<Play> findAllByRatingEnginePid(long pid);

    List<Play> findByRatingEngineAndPlayStatusIn(RatingEngine ratingEngine, List<PlayStatus> statusList);

    Play getPlayByName(String name, Boolean considerDeleted);

    void deleteByName(String name, Boolean hardDelete);

    List<String> getAllDeletedPlayIds(boolean forCleanupOnly);

    Play createPlay(Play play);

    Play updatePlay(Play play, Play existingPlay);

    Long countByPlayTypePid(Long pid);

    List<String> findDisplayNamesCorrespondToPlayNames(List<String> playNames);
}
