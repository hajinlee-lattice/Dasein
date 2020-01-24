package com.latticeengines.modelquality.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.modelquality.dao.AnalyticPipelineDao;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;

@Component("qualityAnalyticPipelineEntityMgr")
public class AnalyticPipelineEntityMgrImpl extends BaseEntityMgrImpl<AnalyticPipeline>
        implements AnalyticPipelineEntityMgr {

    @Inject
    private AnalyticPipelineDao analyticPipelineDao;

    @Override
    public BaseDao<AnalyticPipeline> getDao() {
        return analyticPipelineDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(AnalyticPipeline analyticPipeline) {
        analyticPipeline.setName(analyticPipeline.getName().replace('/', '_'));
        analyticPipelineDao.create(analyticPipeline);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AnalyticPipeline findByName(String analyticPipelineName) {
        return analyticPipelineDao.findByField("NAME", analyticPipelineName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AnalyticPipeline getLatestProductionVersion() {
        return analyticPipelineDao.findByMaxVersion();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AnalyticPipeline findByVersion(Integer version) {
        return analyticPipelineDao.findByField("VERSION", version);
    }
}
