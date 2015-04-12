package com.latticeengines.scoring.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;

public interface ScoringCommandStateEntityMgr extends BaseEntityMgr<ScoringCommandState>{

    List<ScoringCommandState> findByScoringCommandAndStep(ScoringCommand scoringCommand,
            ScoringCommandStep scoringCommandStep);

    List<ScoringCommandState> findByScoringCommand(ScoringCommand scoringCommand);

    ScoringCommandState findLastStateByScoringCommand(ScoringCommand scoringCommand);
}
