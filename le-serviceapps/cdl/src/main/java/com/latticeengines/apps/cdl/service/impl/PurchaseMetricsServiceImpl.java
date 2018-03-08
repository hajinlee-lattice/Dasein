package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.PurchaseMetricsEntityMgr;
import com.latticeengines.apps.cdl.service.PurchaseMetricsService;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

@Service("purchaseMetricsService")
public class PurchaseMetricsServiceImpl implements PurchaseMetricsService {
    @Inject
    private PurchaseMetricsEntityMgr entityMgr;

    @Override
    public List<PurchaseMetrics> findAllActive() {
        return entityMgr.findAllActive();
    }

    @Override
    public List<PurchaseMetrics> save(List<PurchaseMetrics> metricsList) {
        return entityMgr.save(metricsList);
    }
}
