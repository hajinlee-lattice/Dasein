package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.MigrateTracking;

public interface MigrateTrackingService {

    /**
     *
     * @param migrateTracking tracking record tobe created.
     * @return MigrateTracking object with PID
     */
    MigrateTracking create(MigrateTracking migrateTracking);

    /**
     *
     * @param pid MigrateTracking PID
     * @return MigrateTracking record.
     */
    MigrateTracking getByPid(Long pid);

}
