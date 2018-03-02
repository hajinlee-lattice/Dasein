package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PurchaseMetricsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.PurchaseMetrics;

@Component("purchaseMetricsDao")
public class PurchaseMetricsDaoImpl extends BaseDaoImpl<PurchaseMetrics> implements PurchaseMetricsDao {
    @Override
    protected Class<PurchaseMetrics> getEntityClass() {
        return PurchaseMetrics.class;
    }
}
