package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineToPipelineSteps;
import com.latticeengines.modelquality.dao.PipelineDao;
import com.latticeengines.modelquality.dao.PipelineStepDao;
import com.latticeengines.modelquality.dao.PipelineToPipelineStepsDao;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineToPipelineStepsEntityMgr;

@Component("pipelineEntityMgr")
public class PipelineEntityMgrImpl extends BaseEntityMgrImpl<Pipeline> implements PipelineEntityMgr {

    @Autowired
    private PipelineDao pipelineDao;

    @Autowired
    private PipelineToPipelineStepsDao pipelineToPipelineStepsDao;

    @Autowired
    private PipelineToPipelineStepsEntityMgr pipelineToPipelineStepsEntityMgr;

    @Autowired
    private PipelineStepDao pipelineStepDao;

    @Override
    public BaseDao<Pipeline> getDao() {
        return pipelineDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Pipeline pipeline) {
        pipeline.setName(pipeline.getName().replace('/', '_'));
        for (PipelineStep step : pipeline.getPipelineSteps()) {
            String stepName = step.getName().replace('/', '_');
            if (pipelineStepDao.findByField("NAME", stepName) == null) {
                step.setName(stepName);
                pipelineStepDao.create(step);
            }
        }
        pipelineDao.create(pipeline);
        for (PipelineToPipelineSteps ptoPStep : pipeline.getPipelineToPipelineSteps()) {
            pipelineToPipelineStepsEntityMgr.create(ptoPStep);
        }
        setPipelineStepOrder(pipeline);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(Pipeline pipeline) {
        Pipeline exists = findByName(pipeline.getName());
        if (exists == null) {
            return;
        }
        HibernateUtils.inflateDetails(exists.getPipelineToPipelineSteps());
        List<PipelineToPipelineSteps> steps = pipeline.getPipelineToPipelineSteps();
        for (PipelineToPipelineSteps step: steps) {
            pipelineToPipelineStepsDao.delete(step);
        }
        pipelineDao.delete(exists);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Pipeline findByName(String name) {
        return pipelineDao.findByField("NAME", name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Pipeline getLatestProductionVersion() {
        return pipelineDao.findByMaxVersion();
    }

    private void setPipelineStepOrder(Pipeline pipeline) {
        List<PipelineToPipelineSteps> steps = pipeline.getPipelineToPipelineSteps();

        int i = 1;
        for (PipelineToPipelineSteps s : steps) {
            s.setOrder(i++);
            pipelineToPipelineStepsDao.update(s);
        }
    }
}
