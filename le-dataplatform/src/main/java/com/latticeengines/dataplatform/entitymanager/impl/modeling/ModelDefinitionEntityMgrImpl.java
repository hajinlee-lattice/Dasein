package com.latticeengines.dataplatform.entitymanager.impl.modeling;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.modeling.ModelDefinitionDao;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;

@Component("modelDefinitionEntityMgr")
public class ModelDefinitionEntityMgrImpl extends BaseEntityMgrImpl<ModelDefinition> implements ModelDefinitionEntityMgr {

    @Inject
    private ModelDefinitionDao modelDefinitionDao;
        
    public ModelDefinitionEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<ModelDefinition> getDao() {
        return modelDefinitionDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ModelDefinition findByName(String name) {
        return modelDefinitionDao.findByName(name);
    }
    
    
    
}
