package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.PlayType;

public interface PlayTypeEntityMgr extends BaseEntityMgrRepository<PlayType, Long> {

    PlayType findById(String id);

    PlayType findByPid(Long pid);
}
