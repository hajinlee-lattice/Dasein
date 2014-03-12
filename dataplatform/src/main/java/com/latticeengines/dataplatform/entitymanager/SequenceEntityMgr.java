package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.impl.Sequence;


public interface SequenceEntityMgr extends BaseEntityMgr<Sequence> {

    Long nextVal(Class<? extends BaseDao<?>> daoClass);
    
}
