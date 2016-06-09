package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.dao.DataSetDao;

@Component("dataSetEntityMgr")
public class DataSetEntityMgrImpl extends BaseEntityMgrImpl<DataSet> {
    
    @Autowired
    private DataSetDao dataSetDao;
    
    @Override
    public BaseDao<DataSet> getDao() {
        return dataSetDao;
    }

}
