package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.metadata.dao.DataFeedImportDao;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;

@Component("datafeedImportEntityMgr")
public class DataFeedImportEntityMgrImpl extends BaseEntityMgrImpl<DataFeedImport> implements DataFeedImportEntityMgr {

    @Autowired
    private DataFeedImportDao datafeedImportDao;

    @Override
    public BaseDao<DataFeedImport> getDao() {
        return datafeedImportDao;
    }

}
