package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.dao.ModelRunDao;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;

/**
 * This entity manager is responsible for persisting the configuration selected by the user.
 * The dimensional structure contains all the different possible options for the dimension levels and fields, but
 * once a selection is made, there will only be one value for the full hierarchy. 
 * 
 * @author rgonzalez
 *
 */
@Component("modelRunEntityMgr")
public class ModelRunEntityMgrImpl extends BaseEntityMgrImpl<ModelRun> implements ModelRunEntityMgr {

    @Autowired
    private ModelRunDao modelRunDao;
    
    @Override
    public BaseDao<ModelRun> getDao() {
        return modelRunDao;
    }

}
