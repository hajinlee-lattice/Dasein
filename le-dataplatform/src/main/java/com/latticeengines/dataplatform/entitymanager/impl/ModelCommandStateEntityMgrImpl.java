package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelCommandStateDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelCommandStateEntityMgr")
public class ModelCommandStateEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommandState> implements ModelCommandStateEntityMgr {

    @Inject
    private ModelCommandStateDao modelCommandStateDao;
    
    public ModelCommandStateEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommandState> getDao() {
        return modelCommandStateDao;
    }

    @Override
    @Transactional(value="dlorchestration", propagation = Propagation.REQUIRED)
    public List<ModelCommandState> findByModelCommandAndStep(ModelCommand modelCommand, ModelCommandStep modelCommandStep) {
        return modelCommandStateDao.findByModelCommandAndStep(modelCommand, modelCommandStep);
    }

}
