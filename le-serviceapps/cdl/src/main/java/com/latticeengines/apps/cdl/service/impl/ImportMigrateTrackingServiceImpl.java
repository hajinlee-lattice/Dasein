package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ImportMigrateTrackingEntityMgr;
import com.latticeengines.apps.cdl.service.ImportMigrateTrackingService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("importMigrateTrackingService")
public class ImportMigrateTrackingServiceImpl implements ImportMigrateTrackingService {

    @Inject
    private ImportMigrateTrackingEntityMgr importMigrateTrackingEntityMgr;

    @Override
    public ImportMigrateTracking create(String customerSpace, ImportMigrateTracking importMigrateTracking) {
        List<ImportMigrateTracking> importMigrateTrackings = importMigrateTrackingEntityMgr.findAll();
        if (!CollectionUtils.isEmpty(importMigrateTrackings)) {
            for (ImportMigrateTracking tracking : importMigrateTrackings) {
                if (!ImportMigrateTracking.Status.FAILED.equals(tracking.getStatus())) {
                    if (ImportMigrateTracking.Status.COMPLETED.equals(tracking.getStatus())) {
                        throw new RuntimeException("Cannot create another MigrateTracking record, cause the migrate " +
                                "already completed!");
                    } else {
                        throw new RuntimeException("Cannot create another MigrateTracking record, cause there is " +
                                "ongoing migration!");
                    }
                }
            }
        }
        importMigrateTrackingEntityMgr.create(importMigrateTracking);
        return importMigrateTracking;
    }

    @Override
    public ImportMigrateTracking create(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        ImportMigrateTracking importMigrateTracking = new ImportMigrateTracking();
        importMigrateTracking.setTenant(tenant);
        importMigrateTracking.setStatus(ImportMigrateTracking.Status.STARTING);
        return create(customerSpace, importMigrateTracking);
    }

    @Override
    public ImportMigrateTracking getByPid(String customerSpace, Long pid) {
        return importMigrateTrackingEntityMgr.findByPid(pid);
    }

    @Override
    public void updateStatus(String customerSpace, Long pid, ImportMigrateTracking.Status status) {
        ImportMigrateTracking importMigrateTracking = importMigrateTrackingEntityMgr.findByPid(pid);
        if (importMigrateTracking != null) {
            importMigrateTracking.setStatus(status);
            importMigrateTrackingEntityMgr.update(importMigrateTracking);
        }
    }

    @Override
    public void updateReport(String customerSpace, Long pid, ImportMigrateReport report) {
        ImportMigrateTracking importMigrateTracking = importMigrateTrackingEntityMgr.findByPid(pid);
        if (importMigrateTracking != null) {
            importMigrateTracking.setReport(report);
            importMigrateTrackingEntityMgr.update(importMigrateTracking);
        }
    }
}
