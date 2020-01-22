package com.latticeengines.dataplatform.entitymanager.impl.modeling;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.modeling.ModelDao;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modeling.Model;

@Component("modelEntityMgr")
public class ModelEntityMgrImpl extends BaseEntityMgrImpl<Model> implements ModelEntityMgr {

    @Inject
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
