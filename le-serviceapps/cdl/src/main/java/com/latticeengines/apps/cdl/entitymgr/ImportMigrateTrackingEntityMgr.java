package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

public interface ImportMigrateTrackingEntityMgr extends BaseEntityMgrRepository<ImportMigrateTracking, Long> {

    ImportMigrateTracking findByPid(Long pid);
}
