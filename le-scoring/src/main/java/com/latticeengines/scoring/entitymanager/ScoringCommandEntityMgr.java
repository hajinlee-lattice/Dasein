package com.latticeengines.scoring.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;

public interface ScoringCommandEntityMgr extends BaseEntityMgr<ScoringCommand>{

    List<ScoringCommand> getPopulated();

    List<ScoringCommand> getConsumed();

}
