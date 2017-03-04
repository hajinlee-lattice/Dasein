package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.PipelineToPipelineSteps;
import com.latticeengines.modelquality.dao.PipelineToPipelineStepsDao;
import com.latticeengines.modelquality.entitymgr.PipelineToPipelineStepsEntityMgr;

@Component("pipelineToPipelineStepsEntityMgr")
public class PipelineToPipelineStepsEntityMgrImpl extends BaseEntityMgrImpl<PipelineToPipelineSteps>
        implements PipelineToPipelineStepsEntityMgr {

    @Autowired
    private PipelineToPipelineStepsDao pipelineToPipelineStepsDao;

    @Override
    public BaseDao<PipelineToPipelineSteps> getDao() {
        return pipelineToPipelineStepsDao;
    }

}
