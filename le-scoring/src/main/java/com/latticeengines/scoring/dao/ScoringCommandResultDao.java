package com.latticeengines.scoring.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;

public interface ScoringCommandResultDao extends BaseDao<ScoringCommandResult>{

    List<ScoringCommandResult> getConsumed();

}
