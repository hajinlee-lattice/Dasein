package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.PlayGroup;

public interface PlayGroupEntityMgr extends BaseEntityMgrRepository<PlayGroup, Long> {

    PlayGroup findById(String id);

    PlayGroup findByPid(Long pid);
}
