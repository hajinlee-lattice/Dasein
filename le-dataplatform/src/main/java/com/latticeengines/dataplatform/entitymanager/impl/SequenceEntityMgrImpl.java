package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.SequenceDao;
import com.latticeengines.dataplatform.dao.impl.Sequence;
import com.latticeengines.dataplatform.entitymanager.SequenceEntityMgr;

/// @Component("sequenceEntityMgr")
public class SequenceEntityMgrImpl extends BaseEntityMgrImpl  implements SequenceEntityMgr {

    /// @Autowired
    private SequenceDao sequenceDao;
    
    public SequenceEntityMgrImpl() {
        super();
    }

  @Override
    public BaseDao<Sequence> getDao() {
        return null;
    }

    @Override
    public Long nextVal(Class<? extends BaseDao<?>> daoClass) {
        return sequenceDao.nextVal(daoClass.getSimpleName());
    }

}
