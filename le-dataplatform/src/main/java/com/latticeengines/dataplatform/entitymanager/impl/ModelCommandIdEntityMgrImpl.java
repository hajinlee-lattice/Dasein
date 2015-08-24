package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandIdDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandIdEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandId;

@Component("modelCommandIdEntityMgr")
public class ModelCommandIdEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommandId> implements ModelCommandIdEntityMgr{

    @Autowired
    private ModelCommandIdDao modelCommandIdDao;

    @Override
    public BaseDao<ModelCommandId> getDao() {
        return modelCommandIdDao;
    }

}
