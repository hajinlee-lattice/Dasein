package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.MigrateReport;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

public interface MigrateTrackingService {

    /**
     *
     * @param customerSpace Current customerSpace.
     * @return create a MigrateTracking record for current tenant.
     */
    MigrateTracking create(String customerSpace);

    /**
     *
     * @param migrateTracking tracking record tobe created.
     * @return MigrateTracking object with PID
     */
    MigrateTracking create(String customerSpace, MigrateTracking migrateTracking);

    /**
     *
     * @param pid MigrateTracking PID
     * @return MigrateTracking record.
     */
    MigrateTracking getByPid(String customerSpace, Long pid);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @param pid PID for MigrateTracking record.
     * @param status record status
     */
    void updateStatus(String customerSpace, Long pid, MigrateTracking.Status status);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @param pid PID for MigrateTracking record.
     * @param report Migrate report
     */
    void updateReport(String customerSpace, Long pid, MigrateReport report);

}
