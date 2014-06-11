package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelDefinitionDao;
import com.latticeengines.dataplatform.entitymanager.ModelDefinitionEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Component("modelDefinitionEntityMgr")
public class ModelDefinitionEntityMgrImpl extends BaseEntityMgrImpl<ModelDefinition> implements ModelDefinitionEntityMgr {

    @Autowired
    private ModelDefinitionDao modelDefinitionDao;
        
    public ModelDefinitionEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelDefinition> getDao() {
        return modelDefinitionDao;
    }

    @Override
    public ModelDefinition findByName(String name) {
        return modelDefinitionDao.findByName(name);
    }
    
    
    
}
