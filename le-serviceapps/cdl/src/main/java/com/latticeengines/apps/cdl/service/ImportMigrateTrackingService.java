package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

public interface ImportMigrateTrackingService {

    /**
     *
     * @param customerSpace Current customerSpace.
     * @return create a MigrateTracking record for current tenant.
     */
    ImportMigrateTracking create(String customerSpace);

    /**
     *
     * @param importMigrateTracking tracking record tobe created.
     * @return MigrateTracking object with PID
     */
    ImportMigrateTracking create(String customerSpace, ImportMigrateTracking importMigrateTracking);

    /**
     *
     * @param pid MigrateTracking PID
     * @return MigrateTracking record.
     */
    ImportMigrateTracking getByPid(String customerSpace, Long pid);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @return All import migrate tracking records in current tenant.
     */
    List<ImportMigrateTracking> getAll(String customerSpace);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @param pid MigrateTracking PID
     * @return Registered import action ids
     */
    List<Long> getAllRegisteredActionIds(String customerSpace, Long pid);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @param pid PID for MigrateTracking record.
     * @param status record status
     */
    void updateStatus(String customerSpace, Long pid, ImportMigrateTracking.Status status);

    /**
     *
     * @param customerSpace Current customerSpace.
     * @param pid PID for MigrateTracking record.
     * @param report Migrate report
     */
    void updateReport(String customerSpace, Long pid, ImportMigrateReport report);

}
