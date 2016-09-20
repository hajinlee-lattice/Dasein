package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.modelquality.dao.PipelineStepDao;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;

@Component("pipelineStepEntityMgr")
public class PipelineStepEntityMgrImpl extends BaseEntityMgrImpl<PipelineStep> implements PipelineStepEntityMgr {

    @Autowired
    private PipelineStepDao pipelineStepDao;

    @Override
    public BaseDao<PipelineStep> getDao() {
        return pipelineStepDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PipelineStep findByName(String stepName) {
        return pipelineStepDao.findByField("NAME", stepName);
    }

}
