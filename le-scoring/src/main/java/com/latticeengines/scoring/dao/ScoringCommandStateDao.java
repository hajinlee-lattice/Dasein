package com.latticeengines.scoring.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;

public interface ScoringCommandStateDao extends BaseDao<ScoringCommandState> {

    List<ScoringCommandState> findByScoringCommandAndStep(ScoringCommand scoringCommand,
            ScoringCommandStep scoringCommandStep);

    List<ScoringCommandState> findByScoringCommand(ScoringCommand scoringCommand);

    void delete(ScoringCommand scoringCommand);

}
