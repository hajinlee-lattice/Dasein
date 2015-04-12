package com.latticeengines.scoring.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;

public interface ScoringCommandLogDao extends BaseDao<ScoringCommandLog> {

    List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand);

}
