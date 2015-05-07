package com.latticeengines.scoring.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;

public interface ScoringCommandLogEntityMgr extends BaseEntityMgr<ScoringCommandLog>{

    List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand);

    void delete(ScoringCommand scoringCommand);
}
