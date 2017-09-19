package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryMetrics;
import com.latticeengines.modelquality.dao.ModelSummaryMetricsDao;
import com.latticeengines.modelquality.entitymgr.ModelSummaryMetricsEntityMgr;

@Component("modelSummaryMetricsEntityMgr")
public class ModelSummaryMetricsEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryMetrics>
        implements ModelSummaryMetricsEntityMgr {

    @Autowired
    private ModelSummaryMetricsDao modelSummaryMetricsDao;

    @Override
    public BaseDao<ModelSummaryMetrics> getDao() {
        return modelSummaryMetricsDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ModelSummaryMetrics summaryMetrics) {
        modelSummaryMetricsDao.create(summaryMetrics);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(ModelSummaryMetrics summaryMetrics) {
        modelSummaryMetricsDao.delete(summaryMetrics);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummaryMetrics> getAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummaryMetrics> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelSummaryMetrics getByModelId(String id) {
        return modelSummaryMetricsDao.findByField("id", id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateLastUpdateTime(ModelSummaryMetrics summary) {
        summary.setLastUpdateTime(System.currentTimeMillis());
        super.update(summary);
    }

}
