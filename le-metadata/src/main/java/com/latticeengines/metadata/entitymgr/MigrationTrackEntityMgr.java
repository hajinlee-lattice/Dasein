package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.security.Tenant;

public interface MigrationTrackEntityMgr extends BaseEntityMgrRepository<MigrationTrack, Long> {
    MigrationTrack findByTenant(Tenant tenant);
    Boolean tenantInMigration(Tenant tenant);
}
