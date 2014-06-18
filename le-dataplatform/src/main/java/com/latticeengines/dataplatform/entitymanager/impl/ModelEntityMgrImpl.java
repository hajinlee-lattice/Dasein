package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.Model;

@Component("modelEntityMgr")
public class ModelEntityMgrImpl extends BaseEntityMgrImpl<Model> implements ModelEntityMgr {

    @Autowired
    private ModelDao modelDao; 
    
    public ModelEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<Model> getDao() {
        return modelDao;
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Model findByObjectId(String id) {
        return modelDao.findByObjectId(id);
    }
    
    
}
