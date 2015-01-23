package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandParameterDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandParameterEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;

@Component("modelCommandParameterEntityMgr")
public class ModelCommandParameterEntityMgrImpl extends BaseOrchestrationEntityMgrImpl<ModelCommandParameter> implements ModelCommandParameterEntityMgr {

    @Autowired
    private ModelCommandParameterDao modelCommandParameterDao;

    public ModelCommandParameterEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommandParameter> getDao() {
        return modelCommandParameterDao;
    }

}
