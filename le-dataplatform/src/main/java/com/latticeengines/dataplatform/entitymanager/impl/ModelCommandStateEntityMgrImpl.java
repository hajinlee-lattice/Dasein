package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelCommandStateDao;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelCommandStateEntityMgr")
public class ModelCommandStateEntityMgrImpl extends BaseEntityMgrImpl<ModelCommandState> implements ModelCommandStateEntityMgr {

    @Autowired
    private ModelCommandStateDao modelCommandStateDao;
    
    public ModelCommandStateEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelCommandState> getDao() {
        return modelCommandStateDao;
    }

    @Override
    public List<ModelCommandState> findByModelCommandAndStep(ModelCommand modelCommand, ModelCommandStep modelCommandStep) {
        // TODO Auto-generated method stub
        return null;
    }

}
