package com.latticeengines.scoring.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;

public interface ScoringCommandResultDao extends BaseDao<ScoringCommandResult>{

    ScoringCommandResult findByScoringCommand(ScoringCommand scoringCommand);

}
