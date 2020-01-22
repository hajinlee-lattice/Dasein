package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandLogDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

@Component("modelCommandLogEntityMgr")
public class ModelCommandLogEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommandLog> implements ModelCommandLogEntityMgr {

    @Inject
    private ModelCommandLogDao modelCommandLogDao;

    public ModelCommandLogEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommandLog> getDao() {
        return modelCommandLogDao;
    }

    @Override
    public List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand) {
        return modelCommandLogDao.findByModelCommand(modelCommand);
    }

}
