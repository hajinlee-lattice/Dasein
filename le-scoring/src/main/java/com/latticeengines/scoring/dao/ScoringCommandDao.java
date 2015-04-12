package com.latticeengines.scoring.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;

public interface ScoringCommandDao extends BaseDao<ScoringCommand>{

    List<ScoringCommand> getPopulated();

}
