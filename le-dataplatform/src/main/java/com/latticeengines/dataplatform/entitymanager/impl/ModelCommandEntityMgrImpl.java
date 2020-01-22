package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelCommandDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@Component("modelCommandEntityMgr")
public class ModelCommandEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommand> implements ModelCommandEntityMgr {

    @Inject
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
