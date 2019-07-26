package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.security.Tenant;

public interface MigrationTrackEntityMgr extends BaseEntityMgrRepository<MigrationTrack, Long> {
    MigrationTrack findByTenant(Tenant tenant);

    boolean tenantInMigration(Tenant tenant);

    boolean canDeleteOrRenameTable(Tenant tenant, String tableName);

    List<Long> getStartedTenants();
}
