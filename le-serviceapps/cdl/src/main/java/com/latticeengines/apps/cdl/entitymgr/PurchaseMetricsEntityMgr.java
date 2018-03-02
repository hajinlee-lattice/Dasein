package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

public interface PurchaseMetricsEntityMgr extends BaseEntityMgrRepository<PurchaseMetrics, Long> {
    List<PurchaseMetrics> findAll();

    List<PurchaseMetrics> save(List<PurchaseMetrics> metricsList);

    void deleteAll();
}
