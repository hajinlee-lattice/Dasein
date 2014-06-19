package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelCommandDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("modelCommandEntityMgr")
public class ModelCommandEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommand> implements ModelCommandEntityMgr {

    @Autowired
    private ModelCommandDao modelCommandDao;
    
    public ModelCommandEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommand> getDao() {
        return modelCommandDao;
    }

    @Override
    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    public List<ModelCommand> getNewAndInProgress() {
        return modelCommandDao.getNewAndInProgress();
    }

}
