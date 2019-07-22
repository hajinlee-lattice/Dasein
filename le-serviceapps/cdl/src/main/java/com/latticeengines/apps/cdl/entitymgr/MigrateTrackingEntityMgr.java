package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

public interface MigrateTrackingEntityMgr extends BaseEntityMgrRepository<MigrateTracking, Long> {

    MigrateTracking findByPid(Long pid);
}
