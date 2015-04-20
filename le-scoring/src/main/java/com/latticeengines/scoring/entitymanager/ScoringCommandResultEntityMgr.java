package com.latticeengines.scoring.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;

public interface ScoringCommandResultEntityMgr extends BaseEntityMgr<ScoringCommandResult>{

    ScoringCommandResult findByScoringCommand(ScoringCommand scoringCommand);

    List<ScoringCommandResult> getConsumed();

}
