package com.latticeengines.scoring.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;

public interface ScoringCommandResultEntityMgr extends BaseEntityMgr<ScoringCommandResult>{

    List<ScoringCommandResult> getConsumed();

    ScoringCommandResult findByKey(long pid);

}
