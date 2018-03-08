package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

public interface PurchaseMetricsService {
    List<PurchaseMetrics> findAllActive();

    List<PurchaseMetrics> save(List<PurchaseMetrics> metricsList);
}
