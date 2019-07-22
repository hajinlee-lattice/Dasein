package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.MigrateTrackingEntityMgr;
import com.latticeengines.apps.cdl.service.MigrateTrackingService;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

@Component("migrateTrackingService")
public class MigrateTrackingServiceImpl implements MigrateTrackingService {

    @Inject
    private MigrateTrackingEntityMgr migrateTrackingEntityMgr;

    @Override
    public MigrateTracking create(MigrateTracking migrateTracking) {
        List<MigrateTracking> migrateTrackings = migrateTrackingEntityMgr.findAll();
        if (!CollectionUtils.isEmpty(migrateTrackings)) {
            for (MigrateTracking tracking : migrateTrackings) {
                if (!MigrateTracking.Status.FAILED.equals(tracking.getStatus())) {
                    if (MigrateTracking.Status.COMPLETED.equals(tracking.getStatus())) {
                        throw new RuntimeException("Cannot create another MigrateTracking record, cause the migrate " +
                                "already completed!");
                    } else {
                        throw new RuntimeException("Cannot create another MigrateTracking record, cause there is " +
                                "ongoing migration!");
                    }
                }
            }
        }
        migrateTrackingEntityMgr.create(migrateTracking);
        return migrateTracking;
    }

    @Override
    public MigrateTracking getByPid(Long pid) {
        return migrateTrackingEntityMgr.findByPid(pid);
    }
}
