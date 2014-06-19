package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelCommandResultDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;

@Component("modelCommandResultEntityMgr")
public class ModelCommandResultEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommandResult> implements ModelCommandResultEntityMgr {

    @Autowired
    private ModelCommandResultDao modelCommandResultDao;
    
    public ModelCommandResultEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommandResult> getDao() {
        return modelCommandResultDao;
    }

    @Override
    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    public ModelCommandResult findByModelCommand(ModelCommand modelCommand) {
        return modelCommandResultDao.findByModelCommand(modelCommand);
    }  

}
