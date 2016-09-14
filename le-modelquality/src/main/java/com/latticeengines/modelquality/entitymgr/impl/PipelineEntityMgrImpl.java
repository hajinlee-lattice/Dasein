package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineToPipelineSteps;
import com.latticeengines.modelquality.dao.PipelineDao;
import com.latticeengines.modelquality.dao.PipelineStepDao;
import com.latticeengines.modelquality.dao.PipelineToPipelineStepsDao;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;

@Component("pipelineEntityMgr")
public class PipelineEntityMgrImpl extends BaseEntityMgrImpl<Pipeline> implements PipelineEntityMgr {

    @Autowired
    private PipelineDao pipelineDao;
    
    @Autowired
    private PipelineToPipelineStepsDao pipelineToPipelineStepsDao;
    
    @Autowired
    private PipelineStepDao pipelineStepDao;

    @Override
    public BaseDao<Pipeline> getDao() {
        return pipelineDao;
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Pipeline pipeline) {
        for (PipelineStep step : pipeline.getPipelineSteps()) {
            pipelineStepDao.create(step);
        }

        super.create(pipeline);
        setPipelineStepOrder(pipeline);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Pipeline findByName(String name) {
        return pipelineDao.findByField("NAME", name);
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
