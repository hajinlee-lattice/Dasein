package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryMetrics;
import com.latticeengines.modelquality.dao.ModelSummaryMetricsDao;

@Component("modelSummaryMetricsDao")
public class ModelSummaryMetricsDaoImpl extends ModelQualityBaseDaoImpl<ModelSummaryMetrics> implements ModelSummaryMetricsDao {

    @Override
    protected Class<ModelSummaryMetrics> getEntityClass() {
        return ModelSummaryMetrics.class;
    }

}
